#!/usr/bin/env python3
"""
KickClipBot — stream_detector.py
═══════════════════════════════════════════════════════════════
Smart Stream Detector: detecta el estado de cada canal al iniciar
y decide el modo de operación.

FLUJO:
  1. Verificar estado de cada canal simultáneamente
  2. LIVE   → modo live  (análisis de picos en tiempo real)
  3. OFFLINE → modo VOD   (extraer clips del stream anterior)

MODOS:
  LIVE_MODE  (prioridad 1)  — canal en vivo, detectar picos
  VOD_RECENT (prioridad 2)  — stream en últimas 48h
  VOD_OLD    (prioridad 3)  — stream en última semana
  SKIP       (prioridad 99) — no hay contenido útil

Uso:
    python3 stream_detector.py                         → escaneo de todos los canales
    python3 stream_detector.py --channel <canal>       → escaneo de un canal
    python3 stream_detector.py --live-analyze <canal>  → analizar picos en vivo
    python3 stream_detector.py --vod-analyze  <canal>  → extraer mejores clips de VOD
    python3 stream_detector.py --audio-peaks  <video>  → detectar picos de audio
    python3 stream_detector.py --recheck               → re-verificar offlines

Salida: JSON a stdout
"""

import json
import sys
import os
import time
import random
import subprocess
import shutil
import threading
from datetime import datetime, timezone, timedelta
from collections import deque, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print(json.dumps({"error": "curl_cffi no instalado. pip install curl_cffi"}))
    sys.exit(1)

# ─── Configuración ────────────────────────────────────────────────────────────

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH  = os.path.join(SCRIPT_DIR, "config.json")
STATE_PATH   = os.path.join(SCRIPT_DIR, "stream_state.json")

API_BASE     = "https://kick.com/api/v1"
API_V2       = "https://kick.com/api/v2"
COL_TZ       = timezone(timedelta(hours=-5))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

HYPE_EMOTES = [
    "KEKW", "PogChamp", "OMEGALUL", "LULW", "LUL", "MonkaS", "Kappa",
    "PepeHands", "POGGERS", "catJAM", "ICANT", "EZ", "ModTime", "LETSGO",
    "HYPE", "Pog", "PogU", "AYAYA", "FeelsGoodMan", "FeelsBadMan",
]

PRIORITY_QUEUE = {
    "LIVE_MODE":   1,
    "VOD_RECENT":  2,
    "VOD_OLD":     3,
    "SKIP":        99,
}


def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def load_state():
    try:
        with open(STATE_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {"channels": {}, "last_full_scan": None}


def save_state(state):
    try:
        with open(STATE_PATH, "w") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


def now_col():
    return datetime.now(COL_TZ)


def make_session():
    """Crea una sesión curl_cffi con fingerprint Chrome 131."""
    s = cffi_requests.Session(impersonate="chrome131")
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
        "Referer": "https://kick.com/",
    })
    return s


def api_get(session, url, retries=3, backoff=2):
    """GET con reintentos y backoff exponencial."""
    last_err = None
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return None
            last_err = f"HTTP {resp.status_code}"
        except Exception as e:
            last_err = str(e)
        if attempt < retries - 1:
            wait = backoff ** (attempt + 1) + random.uniform(0, 1)
            time.sleep(wait)
    return {"_error": last_err}


def calculate_hours_since(date_str):
    """Calcula horas desde un ISO timestamp."""
    if not date_str:
        return 9999
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt
        return delta.total_seconds() / 3600
    except Exception:
        return 9999


def format_time_ago(hours):
    """Formatea 'hace Xh/Xd' para display."""
    if hours < 1:
        return f"hace {int(hours * 60)}min"
    elif hours < 24:
        return f"hace {int(hours)}h"
    elif hours < 168:
        return f"hace {round(hours / 24, 1)}d"
    else:
        return f"hace {int(hours / 24)}d"


# ══════════════════════════════════════════════════════════════════════════════
# DETECCIÓN DE ESTADO DE CANAL
# ══════════════════════════════════════════════════════════════════════════════

def check_channel_status(channel):
    """
    Verifica el estado de un canal de Kick.
    Retorna info completa de live o del último VOD.
    """
    session = make_session()
    result = {
        "channel": channel,
        "status": "UNKNOWN",
        "mode": "SKIP",
        "priority": PRIORITY_QUEUE["SKIP"],
        "checked_at": now_col().isoformat(),
    }

    # ── 1. Obtener info del canal ──
    channel_data = api_get(session, f"{API_BASE}/channels/{channel}")

    if channel_data is None:
        result["status"] = "NOT_FOUND"
        result["error"] = f"Canal '{channel}' no existe en Kick"
        return result

    if isinstance(channel_data, dict) and "_error" in channel_data:
        result["status"] = "API_ERROR"
        result["error"] = channel_data["_error"]
        return result

    channel_id = channel_data.get("id")
    livestream = channel_data.get("livestream")

    # ── 2. ¿Está en vivo? ──
    if livestream and livestream.get("is_live", False):
        viewers = livestream.get("viewer_count", 0) or livestream.get("viewers", 0)
        result.update({
            "status": "LIVE",
            "mode": "LIVE_MODE",
            "priority": PRIORITY_QUEUE["LIVE_MODE"],
            "channel_id": channel_id,
            "viewers": viewers,
            "title": livestream.get("session_title", ""),
            "started_at": livestream.get("created_at", ""),
            "stream_id": livestream.get("id"),
            "slug": livestream.get("slug", ""),
            "language": livestream.get("language", ""),
            "thumbnail": (livestream.get("thumbnail", {}) or {}).get("url", ""),
            "category": (livestream.get("categories", [{}]) or [{}])[0].get("name", "")
            if isinstance(livestream.get("categories"), list) and livestream.get("categories")
            else (livestream.get("category", {}) or {}).get("name", ""),
            "hours_live": calculate_hours_since(livestream.get("created_at", "")),
        })
        return result

    # ── 3. OFFLINE: buscar VOD/stream anterior ──
    result["status"] = "OFFLINE"
    result["channel_id"] = channel_id

    # Intentar obtener VODs del canal
    vod_data = None
    for vod_url in [
        f"{API_V2}/channels/{channel}/videos",
        f"{API_BASE}/channels/{channel}/videos",
        f"{API_V2}/channels/{channel_id}/videos" if channel_id else None,
    ]:
        if vod_url is None:
            continue
        vod_data = api_get(session, vod_url, retries=2, backoff=1)
        if vod_data and not isinstance(vod_data, dict):
            break
        if isinstance(vod_data, dict) and vod_data.get("data"):
            break
        if isinstance(vod_data, list) and len(vod_data) > 0:
            break

    # Parsear VODs
    vods_list = []
    if isinstance(vod_data, dict) and vod_data.get("data"):
        vods_list = vod_data["data"]
    elif isinstance(vod_data, list):
        vods_list = vod_data

    last_vod = vods_list[0] if vods_list else None

    if last_vod:
        vod_date = last_vod.get("created_at", "") or last_vod.get("start_time", "")
        hours_ago = calculate_hours_since(vod_date)

        result["last_stream"] = {
            "id": last_vod.get("id"),
            "title": last_vod.get("session_title", "") or last_vod.get("title", ""),
            "duration": last_vod.get("duration", 0) or last_vod.get("length", 0),
            "date": vod_date,
            "hours_ago": round(hours_ago, 1),
            "time_ago": format_time_ago(hours_ago),
            "url": last_vod.get("source", "") or last_vod.get("video", {}).get("url", ""),
            "thumbnail": last_vod.get("thumbnail", "") or last_vod.get("thumbnail_url", ""),
            "views": last_vod.get("views", 0) or last_vod.get("view_count", 0),
        }

        # Decidir modo
        if hours_ago <= 48:
            result["mode"] = "VOD_RECENT"
            result["priority"] = PRIORITY_QUEUE["VOD_RECENT"]
        elif hours_ago <= 168:
            result["mode"] = "VOD_OLD"
            result["priority"] = PRIORITY_QUEUE["VOD_OLD"]
        else:
            result["mode"] = "SKIP"
            result["priority"] = PRIORITY_QUEUE["SKIP"]
    else:
        result["last_stream"] = None
        result["mode"] = "SKIP"
        result["priority"] = PRIORITY_QUEUE["SKIP"]

    # ── 4. También buscar clips públicos recientes ──
    clips_data = api_get(session, f"{API_V2}/channels/{channel}/clips", retries=2, backoff=1)
    clips_list = []
    if isinstance(clips_data, dict) and clips_data.get("clips"):
        clips_list = clips_data["clips"]
    elif isinstance(clips_data, list):
        clips_list = clips_data

    recent_clips = []
    for clip in clips_list[:10]:
        clip_date = clip.get("created_at", "")
        clip_hours = calculate_hours_since(clip_date)
        if clip_hours <= 48:
            recent_clips.append({
                "id": clip.get("id"),
                "title": clip.get("title", ""),
                "views": clip.get("view_count", 0) or clip.get("views", 0),
                "likes": clip.get("likes_count", 0) or clip.get("likes", 0),
                "duration": clip.get("duration", 0),
                "url": clip.get("clip_url", "") or clip.get("url", ""),
                "thumbnail": clip.get("thumbnail_url", "") or clip.get("thumbnail", ""),
                "hours_ago": round(clip_hours, 1),
                "creator": clip.get("creator", {}).get("username", "") if isinstance(clip.get("creator"), dict) else "",
            })

    if recent_clips:
        recent_clips.sort(key=lambda c: c.get("views", 0), reverse=True)
        result["recent_clips"] = recent_clips[:5]
        result["recent_clips_count"] = len(recent_clips)

        # Si tiene clips recientes pero no VOD, subir prioridad
        if result["mode"] == "SKIP" and len(recent_clips) > 0:
            result["mode"] = "VOD_RECENT"
            result["priority"] = PRIORITY_QUEUE["VOD_RECENT"]

    session.close()
    return result


def scan_all_channels(channels=None):
    """Escanea todos los canales simultáneamente."""
    config = load_config()
    if channels is None:
        channels = config.get("channels", [])

    results = []

    with ThreadPoolExecutor(max_workers=min(len(channels), 6)) as executor:
        futures = {
            executor.submit(check_channel_status, ch): ch
            for ch in channels
        }
        for future in as_completed(futures):
            ch = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                results.append({
                    "channel": ch,
                    "status": "ERROR",
                    "mode": "SKIP",
                    "priority": 99,
                    "error": str(e)[:100],
                })

    # Ordenar por prioridad
    results.sort(key=lambda r: r.get("priority", 99))

    # Guardar estado
    state = load_state()
    state["last_full_scan"] = now_col().isoformat()
    for r in results:
        state["channels"][r["channel"]] = {
            "status": r["status"],
            "mode": r["mode"],
            "priority": r.get("priority", 99),
            "last_check": r.get("checked_at"),
            "viewers": r.get("viewers", 0),
        }
    save_state(state)

    return results


# ══════════════════════════════════════════════════════════════════════════════
# LIVE ANALYZER — análisis de picos en tiempo real
# ══════════════════════════════════════════════════════════════════════════════

class LiveAnalyzer:
    """
    Analiza un stream en vivo para detectar picos virales.
    Monitorea chat via API polling y viewers para calcular score.
    """

    def __init__(self, channel, channel_id, stream_id=None):
        self.channel = channel
        self.channel_id = channel_id
        self.stream_id = stream_id
        self.chat_window = deque(maxlen=500)
        self.viewer_history = deque(maxlen=60)
        self.peak_moments = []
        self.session = make_session()
        self.config = load_config()
        self.min_score = self.config.get("viral_detection", {}).get("min_viral_score", 7)
        self.emote_counter = Counter()
        self._running = False

    def get_chat_messages(self, window_sec=60):
        """Obtiene mensajes del chat vía API polling."""
        messages = []
        for url in [
            f"{API_V2}/channels/{self.channel_id}/messages",
            f"{API_V2}/channels/{self.channel}/messages",
        ]:
            try:
                data = api_get(self.session, url, retries=1, backoff=1)
                if isinstance(data, dict) and data.get("data"):
                    raw_msgs = data["data"].get("messages", []) if isinstance(data["data"], dict) else data["data"]
                elif isinstance(data, dict) and data.get("messages"):
                    raw_msgs = data["messages"]
                elif isinstance(data, list):
                    raw_msgs = data
                else:
                    continue

                for msg in raw_msgs:
                    content = ""
                    if isinstance(msg, dict):
                        content = msg.get("content", "") or msg.get("message", "")
                    ts = msg.get("created_at", "") if isinstance(msg, dict) else ""
                    messages.append({"content": content, "ts": ts})

                if messages:
                    break
            except Exception:
                continue

        return messages

    def get_current_viewers(self):
        """Obtiene viewers actuales del canal."""
        try:
            data = api_get(self.session, f"{API_BASE}/channels/{self.channel}", retries=1, backoff=1)
            if isinstance(data, dict):
                ls = data.get("livestream")
                if ls:
                    return ls.get("viewer_count", 0) or ls.get("viewers", 0)
        except Exception:
            pass
        return 0

    def calculate_peak_score(self, msgs_per_min, viewers, hype_ratio):
        """
        Calcula score de pico (1-10):
          - Chat speed: 40%
          - Viewers:    30%
          - Hype emotes: 30%
        """
        config = self.config.get("viral_detection", {})
        msgs_threshold = config.get("msgs_per_min_threshold", 50)
        viewers_threshold = config.get("viewers_threshold", 1000)

        # Normalizar cada componente a 0-10
        chat_score = min(msgs_per_min / max(msgs_threshold, 1) * 10, 10)
        viewer_score = min(viewers / max(viewers_threshold, 1) * 10, 10)
        hype_score = min(hype_ratio * 25, 10)  # 40% hype = score 10

        total = (chat_score * 0.4) + (viewer_score * 0.3) + (hype_score * 0.3)
        return round(min(total, 10), 2)

    def analyze_single_snapshot(self):
        """Toma un snapshot y calcula score actual."""
        msgs = self.get_chat_messages(60)
        viewers = self.get_current_viewers()

        msgs_per_min = len(msgs)

        # Contar emotes de hype
        total_words = 0
        hype_count = 0
        for m in msgs:
            words = m.get("content", "").split()
            total_words += len(words)
            for w in words:
                if w.upper() in [e.upper() for e in HYPE_EMOTES]:
                    hype_count += 1

        hype_ratio = hype_count / max(total_words, 1)
        score = self.calculate_peak_score(msgs_per_min, viewers, hype_ratio)

        return {
            "channel": self.channel,
            "mode": "LIVE_ANALYSIS",
            "viral_score": score,
            "msgs_per_min": msgs_per_min,
            "viewers": viewers,
            "hype_ratio": round(hype_ratio, 4),
            "hype_count": hype_count,
            "total_messages": len(msgs),
            "is_peak": score >= self.min_score,
            "timestamp": now_col().isoformat(),
        }

    def get_clip_timestamp(self, peak_time_iso):
        """Retrocede 5s antes del pico para capturar contexto."""
        try:
            dt = datetime.fromisoformat(peak_time_iso)
            clip_start = dt - timedelta(seconds=5)
            return clip_start.isoformat()
        except Exception:
            return peak_time_iso

    def analyze_peaks_continuous(self, duration_sec=300, interval_sec=15):
        """
        Monitorea continuamente durante duration_sec,
        tomando snapshots cada interval_sec.
        Retorna lista de picos detectados.
        """
        self._running = True
        start = time.time()
        peaks = []
        snapshots = []

        while self._running and (time.time() - start) < duration_sec:
            try:
                snap = self.analyze_single_snapshot()
                snapshots.append(snap)

                if snap["is_peak"]:
                    peak = {
                        **snap,
                        "clip_start_time": self.get_clip_timestamp(snap["timestamp"]),
                        "peak_number": len(peaks) + 1,
                    }
                    peaks.append(peak)
                    self.peak_moments.append(peak)

            except Exception:
                pass

            if (time.time() - start) < duration_sec:
                time.sleep(interval_sec)

        self._running = False

        # Calcular resumen
        all_scores = [s["viral_score"] for s in snapshots]
        avg_score = round(sum(all_scores) / max(len(all_scores), 1), 2)
        max_score = max(all_scores) if all_scores else 0

        return {
            "channel": self.channel,
            "mode": "LIVE_CONTINUOUS",
            "duration_sec": round(time.time() - start),
            "snapshots_taken": len(snapshots),
            "peaks_detected": len(peaks),
            "avg_viral_score": avg_score,
            "max_viral_score": max_score,
            "peaks": peaks,
        }

    def stop(self):
        self._running = False

    def close(self):
        self.stop()
        try:
            self.session.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# VOD ANALYZER — extracción de clips del stream anterior
# ══════════════════════════════════════════════════════════════════════════════

class VODAnalyzer:
    """
    Analiza VODs de streams terminados para extraer los mejores clips.

    Estrategias en orden de prioridad:
      1. API de clips de Kick (clips ya creados por la comunidad)
      2. Detección de picos de audio en el VOD (ffmpeg volumedetect)
      3. Descarga con streamlink/yt-dlp + segmentación
    """

    def __init__(self, channel, vod_data=None, channel_id=None):
        self.channel = channel
        self.channel_id = channel_id
        self.vod_data = vod_data or {}
        self.vod_url = self.vod_data.get("url", "")
        self.duration = self.vod_data.get("duration", 0)
        self.session = make_session()
        self.config = load_config()
        self.clips_dir = os.path.join(
            SCRIPT_DIR,
            self.config.get("paths", {}).get("clips_dir", "./clips")
        )
        os.makedirs(self.clips_dir, exist_ok=True)

    def _get_existing_clips(self):
        """Estrategia 1: obtener clips existentes via API de Kick."""
        clips = []
        for url in [
            f"{API_V2}/channels/{self.channel}/clips",
            f"{API_V2}/clips?channel={self.channel}",
        ]:
            try:
                data = api_get(self.session, url, retries=2, backoff=1)
                raw = []
                if isinstance(data, dict) and data.get("clips"):
                    raw = data["clips"]
                elif isinstance(data, list):
                    raw = data
                elif isinstance(data, dict) and data.get("data"):
                    raw = data["data"]

                for clip in raw:
                    clip_date = clip.get("created_at", "")
                    hours_ago = calculate_hours_since(clip_date)
                    if hours_ago <= 48:
                        clips.append({
                            "id": clip.get("id"),
                            "title": clip.get("title", ""),
                            "views": clip.get("view_count", 0) or clip.get("views", 0),
                            "likes": clip.get("likes_count", 0) or clip.get("likes", 0),
                            "duration": clip.get("duration", 0),
                            "url": clip.get("clip_url", "") or clip.get("url", ""),
                            "thumbnail": clip.get("thumbnail_url", "") or clip.get("thumbnail", ""),
                            "hours_ago": round(hours_ago, 1),
                            "source": "kick_api",
                        })

                if clips:
                    break
            except Exception:
                continue

        clips.sort(key=lambda c: c.get("views", 0), reverse=True)
        return clips

    def _download_clip(self, clip_url, filename):
        """Descarga un clip directamente."""
        if not clip_url:
            return None
        out_path = os.path.join(self.clips_dir, filename)
        try:
            resp = self.session.get(clip_url, timeout=60)
            if resp.status_code == 200 and len(resp.content) > 10000:
                with open(out_path, "wb") as f:
                    f.write(resp.content)
                return out_path
        except Exception:
            pass

        # Fallback: streamlink
        try:
            result = subprocess.run(
                ["streamlink", "--output", out_path, clip_url, "best"],
                capture_output=True, text=True, timeout=120
            )
            if os.path.exists(out_path) and os.path.getsize(out_path) > 10000:
                return out_path
        except Exception:
            pass

        # Fallback: yt-dlp
        try:
            result = subprocess.run(
                ["yt-dlp", "-o", out_path, "--no-playlist", clip_url],
                capture_output=True, text=True, timeout=120
            )
            if os.path.exists(out_path) and os.path.getsize(out_path) > 10000:
                return out_path
        except Exception:
            pass

        return None

    def detect_audio_peaks(self, video_path, top_n=5, segment_sec=60):
        """
        Estrategia 2: Detecta picos de audio en un video.
        Divide el video en segmentos y mide el volumen de cada uno.
        """
        if not video_path or not os.path.exists(video_path):
            return []

        # Obtener duración total
        try:
            probe = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "json", video_path],
                capture_output=True, text=True, timeout=30
            )
            probe_data = json.loads(probe.stdout)
            total_dur = float(probe_data.get("format", {}).get("duration", 0))
        except Exception:
            total_dur = self.duration or 0

        if total_dur <= 0:
            return []

        segments = []
        offset = 0

        while offset < total_dur:
            seg_dur = min(segment_sec, total_dur - offset)
            if seg_dur < 10:
                break

            try:
                # Medir volumen medio del segmento
                cmd = [
                    "ffmpeg", "-ss", str(offset), "-t", str(seg_dur),
                    "-i", video_path,
                    "-af", "volumedetect",
                    "-f", "null", "-"
                ]
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=30
                )
                stderr = result.stderr

                # Parsear mean_volume y max_volume
                mean_vol = -99
                max_vol = -99
                for line in stderr.split("\n"):
                    if "mean_volume" in line:
                        try:
                            mean_vol = float(line.split("mean_volume:")[1].strip().split(" ")[0])
                        except Exception:
                            pass
                    if "max_volume" in line:
                        try:
                            max_vol = float(line.split("max_volume:")[1].strip().split(" ")[0])
                        except Exception:
                            pass

                segments.append({
                    "start_sec": offset,
                    "end_sec": offset + seg_dur,
                    "duration": seg_dur,
                    "mean_volume_db": mean_vol,
                    "max_volume_db": max_vol,
                    "intensity": round(max(0, mean_vol + 50), 2),  # Normalizar: -50dB=0, 0dB=50
                })
            except Exception:
                pass

            offset += segment_sec

        # Ordenar por intensidad (audio más alto = reacción/grito)
        segments.sort(key=lambda s: s["intensity"], reverse=True)
        return segments[:top_n]

    def _extract_segment(self, video_path, start_sec, duration_sec, output_name):
        """Extrae un segmento de video con ffmpeg."""
        out_path = os.path.join(self.clips_dir, output_name)
        config = self.config.get("clip_settings", {})
        max_dur = config.get("max_duration_sec", 60)
        actual_dur = min(duration_sec, max_dur)

        try:
            cmd = [
                "ffmpeg", "-y",
                "-ss", str(max(0, start_sec - 5)),  # 5s contexto antes
                "-i", video_path,
                "-t", str(actual_dur),
                "-c:v", "libx264", "-preset", "fast",
                "-c:a", "aac", "-b:a", "192k",
                "-movflags", "+faststart",
                out_path,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if os.path.exists(out_path) and os.path.getsize(out_path) > 10000:
                size_mb = round(os.path.getsize(out_path) / (1024 * 1024), 2)
                return {
                    "path": out_path,
                    "filename": output_name,
                    "size_mb": size_mb,
                    "start_sec": start_sec,
                    "duration": actual_dur,
                }
        except Exception:
            pass
        return None

    def _download_vod(self, vod_url):
        """Descarga el VOD completo usando streamlink o yt-dlp."""
        if not vod_url:
            return None

        ts = int(time.time())
        out_path = os.path.join(self.clips_dir, f"vod_{self.channel}_{ts}.mp4")

        # Intentar streamlink
        try:
            result = subprocess.run(
                ["streamlink", "--output", out_path, vod_url, "best"],
                capture_output=True, text=True, timeout=600
            )
            if os.path.exists(out_path) and os.path.getsize(out_path) > 100000:
                return out_path
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # Fallback yt-dlp
        try:
            result = subprocess.run(
                ["yt-dlp", "-o", out_path, "--no-playlist",
                 "--format", "best[height<=720]", vod_url],
                capture_output=True, text=True, timeout=600
            )
            if os.path.exists(out_path) and os.path.getsize(out_path) > 100000:
                return out_path
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        return None

    def extract_best_clips(self, max_clips=5):
        """
        Pipeline completo de extracción de clips de VOD.
        Usa las 3 estrategias en orden de prioridad.
        """
        results = {
            "channel": self.channel,
            "mode": "VOD_ANALYSIS",
            "strategy_used": None,
            "clips": [],
            "errors": [],
        }

        # ── Estrategia 1: API de clips de Kick ──
        api_clips = self._get_existing_clips()
        if api_clips:
            results["strategy_used"] = "kick_api_clips"
            downloaded = []

            for i, clip in enumerate(api_clips[:max_clips]):
                clip_url = clip.get("url", "")
                if not clip_url:
                    continue

                ts = int(time.time())
                fname = f"vod_clip_{self.channel}_{ts}_{i}.mp4"
                dl_path = self._download_clip(clip_url, fname)

                if dl_path:
                    downloaded.append({
                        "path": dl_path,
                        "filename": fname,
                        "size_mb": round(os.path.getsize(dl_path) / (1024*1024), 2),
                        "title": clip.get("title", ""),
                        "views": clip.get("views", 0),
                        "source": "kick_api",
                        "duration": clip.get("duration", 0),
                    })

            if downloaded:
                results["clips"] = downloaded
                return results

        # ── Estrategia 2: Detección de picos de audio en VOD ──
        if self.vod_url:
            vod_path = self._download_vod(self.vod_url)
            if vod_path:
                peaks = self.detect_audio_peaks(vod_path, top_n=max_clips)

                if peaks:
                    results["strategy_used"] = "audio_peak_detection"
                    extracted = []

                    for i, peak in enumerate(peaks):
                        ts = int(time.time())
                        fname = f"peak_{self.channel}_{ts}_{i}.mp4"
                        seg = self._extract_segment(
                            vod_path,
                            peak["start_sec"],
                            self.config.get("clip_settings", {}).get("preferred_duration_sec", 30),
                            fname,
                        )
                        if seg:
                            seg["audio_intensity"] = peak["intensity"]
                            seg["mean_volume_db"] = peak["mean_volume_db"]
                            seg["source"] = "audio_peak"
                            extracted.append(seg)

                    if extracted:
                        results["clips"] = extracted

                # Limpiar VOD descargado
                try:
                    os.remove(vod_path)
                except Exception:
                    pass

                if results["clips"]:
                    return results

        # ── Estrategia 3: Clips creados en las últimas 48h ──
        if api_clips:
            results["strategy_used"] = "kick_clips_metadata"
            results["clips"] = [{
                "title": c.get("title", ""),
                "views": c.get("views", 0),
                "url": c.get("url", ""),
                "duration": c.get("duration", 0),
                "source": "kick_clips_no_download",
            } for c in api_clips[:max_clips]]
            return results

        results["strategy_used"] = "none"
        results["errors"].append("No se encontraron clips ni VOD descargable")
        return results

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# DECIDE MODE — lógica de decisión completa
# ══════════════════════════════════════════════════════════════════════════════

def decide_mode(channel_status):
    """
    Decide el modo de operación para un canal basándose en su estado.
    Retorna: LIVE_MODE, VOD_RECENT, VOD_OLD, SKIP
    """
    status = channel_status.get("status", "UNKNOWN")

    if status == "LIVE":
        return "LIVE_MODE"

    if status == "OFFLINE":
        last = channel_status.get("last_stream")
        if not last or not last.get("id"):
            # Sin VOD: verificar si tiene clips recientes
            if channel_status.get("recent_clips"):
                return "VOD_RECENT"
            return "SKIP"

        hours_ago = last.get("hours_ago", 9999)
        if hours_ago <= 48:
            return "VOD_RECENT"
        elif hours_ago <= 168:
            return "VOD_OLD"
        else:
            return "SKIP"

    if status == "NOT_FOUND":
        return "SKIP"

    return "SKIP"


def process_channel_by_mode(channel_status):
    """
    Procesa un canal según su modo detectado.
    Retorna resultado con clips extraídos o análisis de picos.
    """
    channel = channel_status["channel"]
    mode = channel_status.get("mode", "SKIP")

    if mode == "LIVE_MODE":
        # Análisis en vivo: tomar snapshot rápido
        analyzer = LiveAnalyzer(
            channel=channel,
            channel_id=channel_status.get("channel_id"),
            stream_id=channel_status.get("stream_id"),
        )
        try:
            result = analyzer.analyze_single_snapshot()
            result["mode"] = "LIVE_MODE"
            return result
        finally:
            analyzer.close()

    elif mode in ("VOD_RECENT", "VOD_OLD"):
        vod_data = channel_status.get("last_stream", {})
        analyzer = VODAnalyzer(
            channel=channel,
            vod_data=vod_data,
            channel_id=channel_status.get("channel_id"),
        )
        try:
            result = analyzer.extract_best_clips(max_clips=3 if mode == "VOD_RECENT" else 1)
            result["mode"] = mode
            result["priority"] = PRIORITY_QUEUE[mode]
            return result
        finally:
            analyzer.close()

    return {
        "channel": channel,
        "mode": "SKIP",
        "priority": 99,
        "message": "Canal sin contenido procesable",
    }


# ══════════════════════════════════════════════════════════════════════════════
# RE-CHECK — ciclo de re-verificación
# ══════════════════════════════════════════════════════════════════════════════

def recheck_offline_channels(channels=None):
    """
    Re-verifica canales que estaban OFFLINE.
    Detecta transiciones OFFLINE→LIVE y LIVE→OFFLINE.
    """
    state = load_state()
    config = load_config()
    if channels is None:
        channels = config.get("channels", [])

    transitions = []

    for ch in channels:
        old_state = state.get("channels", {}).get(ch, {})
        old_status = old_state.get("status", "UNKNOWN")

        new_status_data = check_channel_status(ch)
        new_status = new_status_data.get("status", "UNKNOWN")

        # Detectar transiciones
        if old_status != "LIVE" and new_status == "LIVE":
            transitions.append({
                "channel": ch,
                "transition": "WENT_LIVE",
                "emoji": "⚡",
                "message": f"{ch} acaba de ir en vivo!",
                "viewers": new_status_data.get("viewers", 0),
                "data": new_status_data,
            })
        elif old_status == "LIVE" and new_status != "LIVE":
            transitions.append({
                "channel": ch,
                "transition": "WENT_OFFLINE",
                "emoji": "📼",
                "message": f"{ch} terminó stream → analizando VOD",
                "data": new_status_data,
            })

        # Actualizar estado
        state["channels"][ch] = {
            "status": new_status,
            "mode": new_status_data.get("mode", "SKIP"),
            "priority": new_status_data.get("priority", 99),
            "last_check": new_status_data.get("checked_at"),
            "viewers": new_status_data.get("viewers", 0),
        }

    state["last_recheck"] = now_col().isoformat()
    save_state(state)

    return {
        "rechecked": len(channels),
        "transitions": transitions,
        "channels": [state["channels"].get(ch, {}) for ch in channels],
        "timestamp": now_col().isoformat(),
    }


# ══════════════════════════════════════════════════════════════════════════════
# PANTALLA DE INICIO
# ══════════════════════════════════════════════════════════════════════════════

def format_startup_screen(scan_results):
    """Genera la pantalla de inicio formateada (datos para JS)."""
    live_channels = [r for r in scan_results if r.get("status") == "LIVE"]
    offline_channels = [r for r in scan_results if r.get("status") == "OFFLINE"]
    error_channels = [r for r in scan_results if r.get("status") not in ("LIVE", "OFFLINE")]

    vod_recent = [r for r in offline_channels if r.get("mode") == "VOD_RECENT"]
    vod_old = [r for r in offline_channels if r.get("mode") == "VOD_OLD"]
    skip = [r for r in scan_results if r.get("mode") == "SKIP"]

    channel_lines = []
    for r in scan_results:
        ch = r["channel"]
        status = r.get("status", "UNKNOWN")

        if status == "LIVE":
            viewers = r.get("viewers", 0)
            line = f"🔴 EN VIVO  — {viewers:,} viewers"
        elif status == "OFFLINE":
            last = r.get("last_stream")
            if last and last.get("hours_ago") is not None:
                line = f"⚫ OFFLINE  — último stream: {last['time_ago']}"
            elif r.get("recent_clips"):
                line = f"⚫ OFFLINE  — {r['recent_clips_count']} clips recientes"
            else:
                line = "⚫ OFFLINE  — sin datos recientes"
        elif status == "NOT_FOUND":
            line = "❓ NO ENCONTRADO"
        else:
            line = f"⚠️ ERROR    — {r.get('error', '?')[:30]}"

        channel_lines.append({"channel": ch, "display": line})

    return {
        "channels": channel_lines,
        "summary": {
            "total": len(scan_results),
            "live": len(live_channels),
            "offline": len(offline_channels),
            "vod_recent": len(vod_recent),
            "vod_old": len(vod_old),
            "skip": len(skip),
            "errors": len(error_channels),
        },
        "live_channels": [r["channel"] for r in live_channels],
        "vod_channels": [r["channel"] for r in vod_recent + vod_old],
        "modes": {r["channel"]: r.get("mode", "SKIP") for r in scan_results},
        "priorities": {r["channel"]: r.get("priority", 99) for r in scan_results},
        "full_data": scan_results,
    }


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    args = sys.argv[1:]

    if len(args) == 0:
        # Escaneo completo de todos los canales
        results = scan_all_channels()
        screen = format_startup_screen(results)
        print(json.dumps(screen, ensure_ascii=False))
        return

    if args[0] == "--channel" and len(args) > 1:
        result = check_channel_status(args[1])
        print(json.dumps(result, ensure_ascii=False))
        return

    if args[0] == "--live-analyze" and len(args) > 1:
        channel = args[1]
        status = check_channel_status(channel)
        if status.get("status") != "LIVE":
            print(json.dumps({"error": f"{channel} no está en vivo", "status": status.get("status")}))
            return
        analyzer = LiveAnalyzer(
            channel=channel,
            channel_id=status.get("channel_id"),
            stream_id=status.get("stream_id"),
        )
        try:
            duration = int(args[2]) if len(args) > 2 else 120
            result = analyzer.analyze_peaks_continuous(duration_sec=duration, interval_sec=15)
            print(json.dumps(result, ensure_ascii=False))
        finally:
            analyzer.close()
        return

    if args[0] == "--vod-analyze" and len(args) > 1:
        channel = args[1]
        status = check_channel_status(channel)
        vod_data = status.get("last_stream", {})
        analyzer = VODAnalyzer(
            channel=channel,
            vod_data=vod_data,
            channel_id=status.get("channel_id"),
        )
        try:
            result = analyzer.extract_best_clips(max_clips=5)
            print(json.dumps(result, ensure_ascii=False))
        finally:
            analyzer.close()
        return

    if args[0] == "--audio-peaks" and len(args) > 1:
        video_path = args[1]
        if not os.path.exists(video_path):
            print(json.dumps({"error": f"Archivo no encontrado: {video_path}"}))
            return
        analyzer = VODAnalyzer(channel="test")
        peaks = analyzer.detect_audio_peaks(video_path, top_n=10)
        print(json.dumps(peaks, ensure_ascii=False))
        analyzer.close()
        return

    if args[0] == "--recheck":
        result = recheck_offline_channels()
        print(json.dumps(result, ensure_ascii=False))
        return

    if args[0] == "--process" and len(args) > 1:
        channel = args[1]
        status = check_channel_status(channel)
        result = process_channel_by_mode(status)
        print(json.dumps(result, ensure_ascii=False))
        return

    # Default: scan all
    results = scan_all_channels()
    screen = format_startup_screen(results)
    print(json.dumps(screen, ensure_ascii=False))


if __name__ == "__main__":
    main()
