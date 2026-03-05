#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════╗
║              Clip Extractor v4.0 — Live + VOD Only             ║
║   NUNCA usa API de clips. SIEMPRE corta del stream HLS/VOD.   ║
╚══════════════════════════════════════════════════════════════════╝

MODOS:
  LIVE  → captura clip directamente del stream HLS en vivo
  VOD   → analiza audio del último VOD, detecta picos, corta clips

REGLAS ESTRICTAS:
  - NUNCA usar GET /api/v2/clips ni endpoint de clips del canal
  - NUNCA usar clips pre-existentes
  - SIEMPRE cortar directamente del stream HLS o VOD HLS
  - SIEMPRE detectar picos de audio antes de cortar
  - Máximo 5 clips por canal por ciclo
  - Mínimo 90 segundos entre timestamps de clips del mismo stream
  - Clips de 50-55 segundos (óptimo para TikTok)

USO:
  python3 clip_extractor.py <canal>                → auto (live o VOD)
  python3 clip_extractor.py <canal> --live         → forzar modo live
  python3 clip_extractor.py <canal> --vod          → forzar modo VOD
  python3 clip_extractor.py <canal> --max-clips 3  → máx clips a extraer
  python3 clip_extractor.py <canal> --best         → 1 mejor clip (compat)
  python3 clip_extractor.py --scan                 → escanear todos los canales

DEPENDENCIAS:
  pip install streamlink curl_cffi
  ffmpeg, ffprobe en PATH
  yt-dlp (fallback opcional)

Salida: JSON a stdout
"""

import json
import sys
import os
import re
import time
import random
import subprocess
import shutil
from datetime import datetime, timezone, timedelta

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print(json.dumps({"error": "curl_cffi no instalado. pip install curl_cffi"}))
    sys.exit(1)

# ─── Configuración ───────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")

API_BASE = "https://kick.com/api/v1"
API_V2 = "https://kick.com/api/v2"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

# Clip settings
DEFAULT_CLIP_DURATION = 55    # Duración óptima para TikTok
MIN_CLIP_GAP_SEC = 90         # Mínimo entre clips del mismo stream
MAX_CLIPS_PER_CHANNEL = 5
AUDIO_PEAK_THRESHOLD_DB = -20 # RMS > -20dB = momento de alto volumen

# Directorios
CLIPS_RAW_DIR = os.path.join(SCRIPT_DIR, "clips", "raw")
BUFFER_DIR = os.path.join(SCRIPT_DIR, "buffer")
CLIPS_DIR = os.path.join(SCRIPT_DIR, "clips")

for d in [CLIPS_RAW_DIR, BUFFER_DIR, CLIPS_DIR]:
    os.makedirs(d, exist_ok=True)


def log(msg):
    print(f"[clip_extractor] {msg}", file=sys.stderr, flush=True)


def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def make_session():
    s = cffi_requests.Session(impersonate="chrome131")
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
        "Referer": "https://kick.com/",
    })
    return s


def api_get(session, url, retries=3, backoff=2):
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


def has_tool(name):
    """Verifica si una herramienta CLI está disponible."""
    return shutil.which(name) is not None


def get_video_duration(path):
    """Obtiene duración de un video en segundos con ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "json", path],
            capture_output=True, text=True, timeout=30
        )
        data = json.loads(result.stdout)
        return float(data.get("format", {}).get("duration", 0))
    except Exception:
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# CHANNEL STATUS — detectar si está LIVE u OFFLINE + datos del VOD
# ══════════════════════════════════════════════════════════════════════════════

def check_channel(channel):
    """
    Verifica estado de un canal Kick.
    Retorna: {status: LIVE|OFFLINE, stream_url, vod_data, ...}
    """
    session = make_session()
    result = {
        "channel": channel,
        "status": "UNKNOWN",
        "stream_url": None,
        "vod_data": None,
        "channel_id": None,
    }

    try:
        data = api_get(session, f"{API_BASE}/channels/{channel}")
        if not data or (isinstance(data, dict) and data.get("_error")):
            result["status"] = "ERROR"
            result["error"] = data.get("_error", "Canal no encontrado") if isinstance(data, dict) else "Canal no encontrado"
            session.close()
            return result

        result["channel_id"] = data.get("id")
        livestream = data.get("livestream")

        # ¿Está en vivo?
        if livestream and livestream.get("is_live", False):
            result["status"] = "LIVE"
            result["viewers"] = livestream.get("viewer_count", 0) or livestream.get("viewers", 0)
            result["title"] = livestream.get("session_title", "")
            result["stream_id"] = livestream.get("id")
            result["started_at"] = livestream.get("created_at", "")
        else:
            result["status"] = "OFFLINE"

            # Buscar último VOD
            vod_data = None
            for vod_url in [
                f"{API_V2}/channels/{channel}/videos",
                f"{API_BASE}/channels/{channel}/videos",
            ]:
                vod_resp = api_get(session, vod_url, retries=2, backoff=1)
                if isinstance(vod_resp, dict) and vod_resp.get("data"):
                    vod_data = vod_resp["data"]
                    break
                elif isinstance(vod_resp, list) and len(vod_resp) > 0:
                    vod_data = vod_resp
                    break

            if vod_data and len(vod_data) > 0:
                vod = vod_data[0]
                vod_date = vod.get("created_at", "") or vod.get("start_time", "")

                # Calcular horas desde el VOD
                hours_ago = 9999
                try:
                    dt = datetime.fromisoformat(vod_date.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    hours_ago = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
                except Exception:
                    pass

                result["vod_data"] = {
                    "id": vod.get("id"),
                    "title": vod.get("session_title", "") or vod.get("title", ""),
                    "duration": vod.get("duration", 0) or vod.get("length", 0),
                    "url": vod.get("source", "") or vod.get("video", {}).get("url", ""),
                    "date": vod_date,
                    "hours_ago": round(hours_ago, 1),
                    "views": vod.get("views", 0) or vod.get("view_count", 0),
                }

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)[:200]

    try:
        session.close()
    except Exception:
        pass

    return result


# ══════════════════════════════════════════════════════════════════════════════
# LIVE CLIPPER — cortar directamente del stream en vivo
# ══════════════════════════════════════════════════════════════════════════════

class LiveClipper:
    """
    Captura clips directamente del stream HLS en vivo.
    Usa streamlink para obtener la URL HLS y ffmpeg para grabar segmentos.
    """

    def __init__(self, channel):
        self.channel = channel
        self.clips_dir = os.path.join(CLIPS_RAW_DIR, channel)
        os.makedirs(self.clips_dir, exist_ok=True)

    def get_stream_url(self):
        """Obtiene URL HLS real del stream via streamlink."""
        if not has_tool("streamlink"):
            log("❌ streamlink no instalado")
            return None

        try:
            result = subprocess.run(
                ["streamlink", f"https://kick.com/{self.channel}", "best", "--stream-url"],
                capture_output=True, text=True, timeout=30
            )
            url = result.stdout.strip()
            if url and "http" in url:
                log(f"✅ Stream URL obtenida para {self.channel}")
                return url
            else:
                log(f"⚠️ streamlink no retornó URL: {result.stderr[:100]}")
        except subprocess.TimeoutExpired:
            log("⚠️ Timeout obteniendo stream URL")
        except FileNotFoundError:
            log("❌ streamlink no encontrado en PATH")
        except Exception as e:
            log(f"⚠️ Error streamlink: {str(e)[:100]}")

        return None

    def capture_clip(self, duration=DEFAULT_CLIP_DURATION):
        """
        Captura un clip del stream en vivo.
        Graba 'duration' segundos del stream actual.

        Returns:
            path del clip o None
        """
        stream_url = self.get_stream_url()
        if not stream_url:
            return None

        ts = int(time.time())
        output = os.path.join(self.clips_dir, f"{self.channel}_live_{ts}.mp4")

        log(f"📹 Grabando {duration}s del stream en vivo de {self.channel}...")

        try:
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-i", stream_url,
                    "-t", str(duration),
                    "-c", "copy",
                    "-avoid_negative_ts", "make_zero",
                    "-movflags", "+faststart",
                    "-y",
                    output,
                ],
                capture_output=True, text=True, timeout=duration + 60
            )

            if os.path.exists(output) and os.path.getsize(output) > 50000:
                size_mb = os.path.getsize(output) / (1024 * 1024)
                log(f"✅ Clip live capturado: {os.path.basename(output)} ({size_mb:.1f}MB)")
                return output
            else:
                log(f"❌ Clip live vacío o muy pequeño")
                if result.stderr:
                    log(f"   ffmpeg: {result.stderr[-200:]}")
                return None

        except subprocess.TimeoutExpired:
            log(f"⚠️ Timeout grabando clip live ({duration + 60}s)")
            if os.path.exists(output) and os.path.getsize(output) > 50000:
                return output
            return None
        except Exception as e:
            log(f"❌ Error capturando clip live: {str(e)[:100]}")
            return None

    def record_buffer(self, duration_minutes=10):
        """
        Graba un buffer continuo del stream en vivo en background.
        Útil para recortar después cuando se detecta un pico viral.

        Returns:
            path del buffer o None
        """
        stream_url = self.get_stream_url()
        if not stream_url:
            return None

        buffer_path = os.path.join(BUFFER_DIR, f"{self.channel}_live.ts")

        log(f"📹 Iniciando buffer de {duration_minutes}min para {self.channel}...")

        try:
            proc = subprocess.Popen(
                [
                    "ffmpeg",
                    "-i", stream_url,
                    "-t", str(duration_minutes * 60),
                    "-c", "copy",
                    "-y",
                    buffer_path,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            log(f"✅ Buffer activo: {self.channel} (PID {proc.pid}, {duration_minutes}min)")
            return buffer_path
        except Exception as e:
            log(f"❌ Error iniciando buffer: {str(e)[:100]}")
            return None

    def clip_from_buffer(self, buffer_path, start_sec, duration=DEFAULT_CLIP_DURATION):
        """
        Corta un clip del buffer previamente grabado.

        Returns:
            path del clip o None
        """
        if not buffer_path or not os.path.exists(buffer_path):
            return None

        ts = int(time.time())
        output = os.path.join(self.clips_dir, f"{self.channel}_buf_{ts}.mp4")

        try:
            subprocess.run(
                [
                    "ffmpeg",
                    "-ss", str(max(0, start_sec - 5)),
                    "-i", buffer_path,
                    "-t", str(duration),
                    "-c:v", "libx264", "-preset", "ultrafast",
                    "-c:a", "aac", "-b:a", "128k",
                    "-avoid_negative_ts", "make_zero",
                    "-movflags", "+faststart",
                    "-y",
                    output,
                ],
                capture_output=True, text=True, timeout=120
            )

            if os.path.exists(output) and os.path.getsize(output) > 50000:
                size_mb = os.path.getsize(output) / (1024 * 1024)
                log(f"✅ Clip de buffer: {os.path.basename(output)} ({size_mb:.1f}MB)")
                return output
        except Exception as e:
            log(f"❌ Error cortando buffer: {str(e)[:100]}")

        return None


# ══════════════════════════════════════════════════════════════════════════════
# VOD CLIPPER — cortar clips del VOD del último stream
# ══════════════════════════════════════════════════════════════════════════════

class VODClipper:
    """
    Analiza el VOD del último stream para detectar momentos virales
    (picos de audio) y corta clips de esos momentos.

    NUNCA usa clips API. SIEMPRE analiza y corta del VOD real.
    """

    def __init__(self, channel, vod_data):
        self.channel = channel
        self.vod_data = vod_data or {}
        self.vod_id = self.vod_data.get("id")
        self.vod_source_url = self.vod_data.get("url", "")
        self.duration = self.vod_data.get("duration", 0)
        self.clips_dir = os.path.join(CLIPS_RAW_DIR, channel)
        os.makedirs(self.clips_dir, exist_ok=True)
        self._cached_vod_url = None

    def get_vod_url(self):
        """
        Obtiene URL HLS del VOD usando streamlink.
        Fallback con yt-dlp si streamlink falla.
        Cachea el resultado para no repetir la consulta.
        """
        if self._cached_vod_url:
            return self._cached_vod_url

        # Si ya tenemos una URL directa del API (HLS .m3u8)
        if self.vod_source_url and ".m3u8" in self.vod_source_url:
            log(f"✅ URL HLS directa del VOD")
            self._cached_vod_url = self.vod_source_url
            return self._cached_vod_url

        # Intentar streamlink
        if has_tool("streamlink") and self.vod_id:
            for url_template in [
                f"https://kick.com/{self.channel}/videos/{self.vod_id}",
                f"https://kick.com/video/{self.vod_id}",
            ]:
                try:
                    result = subprocess.run(
                        ["streamlink", url_template, "best", "--stream-url"],
                        capture_output=True, text=True, timeout=30
                    )
                    url = result.stdout.strip()
                    if url and "http" in url:
                        log(f"✅ VOD URL via streamlink")
                        self._cached_vod_url = url
                        return url
                except Exception:
                    continue

        # Fallback: yt-dlp
        if has_tool("yt-dlp") and self.vod_id:
            for url_template in [
                f"https://kick.com/{self.channel}/videos/{self.vod_id}",
                f"https://kick.com/video/{self.vod_id}",
            ]:
                try:
                    result = subprocess.run(
                        ["yt-dlp", "--get-url", url_template],
                        capture_output=True, text=True, timeout=30
                    )
                    url = result.stdout.strip()
                    if url and "http" in url:
                        log(f"✅ VOD URL via yt-dlp")
                        self._cached_vod_url = url
                        return url
                except Exception:
                    continue

        # Fallback: URL directa del source
        if self.vod_source_url and "http" in self.vod_source_url:
            log(f"⚠️ Usando URL directa del API como fallback")
            self._cached_vod_url = self.vod_source_url
            return self.vod_source_url

        log(f"❌ No se pudo obtener URL del VOD de {self.channel}")
        return None

    def detect_peak_moments(self, vod_url=None):
        """
        Analiza audio del VOD para detectar momentos de alto volumen.
        Gritos/reacciones = audio alto = momento viral.

        Returns:
            Lista de {timestamp, rms_level, score} ordenados por nivel
        """
        url = vod_url or self.get_vod_url()
        if not url:
            return []

        log(f"🔍 Analizando audio del VOD de {self.channel}...")

        # Método 1: astats con RMS level
        try:
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-i", url,
                    "-af", "astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level:file=-",
                    "-f", "null",
                    "-",
                ],
                capture_output=True, text=True, timeout=600
            )

            peaks = self._parse_audio_peaks(result.stderr)

            if peaks:
                log(f"📊 {len(peaks)} picos de audio detectados en {self.channel}")
                return peaks

            log(f"⚠️ astats no retornó picos, probando volumedetect...")

        except subprocess.TimeoutExpired:
            log(f"⚠️ Timeout astats (600s), probando volumedetect...")
        except Exception as e:
            log(f"⚠️ Error astats: {str(e)[:100]}")

        # Método 2: volumedetect por segmentos
        return self._detect_peaks_by_segments(url)

    def _parse_audio_peaks(self, ffmpeg_output):
        """Parsea output de ffmpeg astats para encontrar timestamps con audio alto."""
        peaks = []
        lines = ffmpeg_output.split("\n")

        for i, line in enumerate(lines):
            if "RMS_level" in line:
                try:
                    rms_match = re.search(r"RMS_level=(-?\d+\.?\d*)", line)
                    if not rms_match:
                        continue
                    rms = float(rms_match.group(1))

                    # Buscar timestamp en líneas cercanas
                    timestamp = None
                    for j in range(max(0, i - 3), min(len(lines), i + 3)):
                        time_match = re.search(r"pts_time:(\d+\.?\d*)", lines[j])
                        if time_match:
                            timestamp = float(time_match.group(1))
                            break

                    if timestamp is None:
                        continue

                    # Solo picos por encima del threshold
                    if rms > AUDIO_PEAK_THRESHOLD_DB:
                        score = min(10, (rms + 40) / 2)
                        peaks.append({
                            "timestamp": timestamp,
                            "rms_level": rms,
                            "score": round(score, 2),
                        })
                except Exception:
                    continue

        # Ordenar por nivel de audio descendente
        peaks.sort(key=lambda x: x["rms_level"], reverse=True)

        # Filtrar clips solapados (mínimo MIN_CLIP_GAP_SEC entre clips)
        filtered = []
        for peak in peaks:
            if not any(abs(peak["timestamp"] - p["timestamp"]) < MIN_CLIP_GAP_SEC for p in filtered):
                filtered.append(peak)

        return filtered[:MAX_CLIPS_PER_CHANNEL * 2]

    def _detect_peaks_by_segments(self, vod_url, segment_sec=60):
        """
        Fallback: analiza audio por segmentos usando volumedetect.
        """
        log(f"🔍 Analizando por segmentos de {segment_sec}s...")

        # Obtener duración total
        total_dur = self.duration
        if not total_dur or total_dur <= 0:
            try:
                probe = subprocess.run(
                    ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                     "-of", "json", "-i", vod_url],
                    capture_output=True, text=True, timeout=30
                )
                total_dur = float(json.loads(probe.stdout).get("format", {}).get("duration", 0))
            except Exception:
                total_dur = 7200  # Asumir 2h si no se puede obtener

        if total_dur <= 0:
            return []

        segments = []
        offset = 0
        max_segments = 120  # Máx 120 segmentos (2h @ 60s)

        while offset < total_dur and len(segments) < max_segments:
            seg_dur = min(segment_sec, total_dur - offset)
            if seg_dur < 10:
                break

            try:
                cmd = [
                    "ffmpeg", "-ss", str(offset), "-t", str(seg_dur),
                    "-i", vod_url,
                    "-af", "volumedetect",
                    "-f", "null", "-",
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

                mean_vol = -99
                max_vol = -99
                for line in result.stderr.split("\n"):
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

                intensity = max(0, mean_vol + 50)
                segments.append({
                    "timestamp": offset + seg_dur / 2,
                    "start_sec": offset,
                    "rms_level": mean_vol,
                    "max_vol": max_vol,
                    "intensity": round(intensity, 2),
                    "score": round(min(10, intensity / 5), 2),
                })
            except Exception:
                pass

            offset += segment_sec

        # Ordenar por intensidad
        segments.sort(key=lambda s: s["intensity"], reverse=True)

        # Filtrar solapados
        filtered = []
        for seg in segments:
            if not any(abs(seg["timestamp"] - p["timestamp"]) < MIN_CLIP_GAP_SEC for p in filtered):
                filtered.append(seg)

        log(f"📊 {len(filtered)} segmentos de alto audio en {self.channel}")
        return filtered[:MAX_CLIPS_PER_CHANNEL * 2]

    def cut_clip(self, timestamp, duration=DEFAULT_CLIP_DURATION):
        """
        Corta un clip del VOD en un timestamp específico.
        Retrocede 5s antes del pico para dar contexto.

        Returns:
            path del clip o None
        """
        vod_url = self.get_vod_url()
        if not vod_url:
            return None

        start = max(0, timestamp - 5)
        ts_int = int(timestamp)
        output = os.path.join(self.clips_dir, f"{self.channel}_vod_{ts_int}_{int(time.time())}.mp4")

        log(f"✂️ Cortando clip en t={ts_int}s (dur={duration}s)...")

        try:
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-ss", str(start),
                    "-i", vod_url,
                    "-t", str(duration),
                    "-c:v", "libx264", "-preset", "ultrafast",
                    "-c:a", "aac", "-b:a", "128k",
                    "-avoid_negative_ts", "make_zero",
                    "-movflags", "+faststart",
                    "-y",
                    output,
                ],
                capture_output=True, text=True, timeout=180
            )

            if os.path.exists(output) and os.path.getsize(output) > 50000:
                size_mb = os.path.getsize(output) / (1024 * 1024)
                log(f"✅ Clip VOD cortado: {os.path.basename(output)} ({size_mb:.1f}MB, t={ts_int}s)")
                return output
            else:
                log(f"❌ Clip VOD vacío en t={ts_int}s")
                if result.stderr:
                    log(f"   ffmpeg: {result.stderr[-150:]}")
                return None

        except subprocess.TimeoutExpired:
            log(f"⚠️ Timeout cortando clip en t={ts_int}s")
            if os.path.exists(output) and os.path.getsize(output) > 50000:
                return output
            return None
        except Exception as e:
            log(f"❌ Error cortando clip: {str(e)[:100]}")
            return None

    def extract_best_clips(self, max_clips=3):
        """
        Pipeline completo: analizar audio → detectar picos → cortar clips.

        Returns:
            Lista de {path, channel, timestamp, score, source, size_mb}
        """
        log(f"🎬 Analizando VOD de {self.channel}...")

        vod_url = self.get_vod_url()
        if not vod_url:
            log(f"❌ Sin URL de VOD para {self.channel}")
            return []

        peaks = self.detect_peak_moments(vod_url)

        if not peaks:
            log(f"⚠️ No se detectaron picos en {self.channel}, cortando segmento aleatorio...")
            # Fallback: segmento aleatorio
            rand_ts = random.randint(60, max(120, self.duration - 120)) if self.duration > 240 else 60
            clip_path = self.cut_clip(rand_ts)
            if clip_path:
                return [{
                    "path": clip_path,
                    "channel": self.channel,
                    "timestamp": rand_ts,
                    "score": 3.0,
                    "source": "VOD_random",
                    "size_mb": round(os.path.getsize(clip_path) / (1024 * 1024), 2),
                }]
            return []

        log(f"📊 {len(peaks)} momentos detectados, cortando top {max_clips}...")

        clips = []
        for peak in peaks[:max_clips]:
            clip_path = self.cut_clip(peak["timestamp"])
            if clip_path:
                clips.append({
                    "path": clip_path,
                    "channel": self.channel,
                    "timestamp": peak["timestamp"],
                    "score": peak.get("score", 5.0),
                    "rms_level": peak.get("rms_level", -30),
                    "source": "VOD_audio_peak",
                    "size_mb": round(os.path.getsize(clip_path) / (1024 * 1024), 2),
                })

        log(f"✅ {len(clips)} clips extraídos del VOD de {self.channel}")
        return clips


# ══════════════════════════════════════════════════════════════════════════════
# FLUJO COMPLETO — procesa un canal según su estado
# ══════════════════════════════════════════════════════════════════════════════

def process_channel(channel, force_mode=None, max_clips=3):
    """
    Procesa un canal: detecta estado, elige modo, extrae clips.

    Args:
        channel: nombre del canal Kick
        force_mode: "live" o "vod" para forzar modo
        max_clips: máximo clips a extraer

    Returns:
        dict con {channel, status, mode, clips[], best_clip, error}
    """
    log(f"{'═' * 50}")
    log(f"📺 Procesando: {channel}")
    log(f"{'═' * 50}")

    result = {
        "channel": channel,
        "status": "UNKNOWN",
        "mode": None,
        "clips": [],
        "best_clip": None,
        "error": None,
    }

    # Obtener estado del canal
    ch_status = check_channel(channel)
    result["status"] = ch_status["status"]

    if ch_status["status"] == "ERROR":
        result["error"] = ch_status.get("error", "Error desconocido")
        return result

    # Decidir modo
    if force_mode == "live" or (force_mode is None and ch_status["status"] == "LIVE"):
        result["mode"] = "LIVE"
        log(f"🔴 {channel} → modo LIVE (viewers: {ch_status.get('viewers', '?')})")

        clipper = LiveClipper(channel)
        clip_path = clipper.capture_clip(duration=DEFAULT_CLIP_DURATION)

        if clip_path:
            clip_info = {
                "path": clip_path,
                "channel": channel,
                "timestamp": 0,
                "score": 8.0,
                "source": "LIVE_capture",
                "size_mb": round(os.path.getsize(clip_path) / (1024 * 1024), 2),
            }
            result["clips"] = [clip_info]
            result["best_clip"] = clip_info
        else:
            result["error"] = "No se pudo capturar clip del stream en vivo"

    elif force_mode == "vod" or ch_status["status"] == "OFFLINE":
        result["mode"] = "VOD"
        vod_data = ch_status.get("vod_data")

        if not vod_data:
            result["error"] = f"{channel} está offline y no tiene VODs disponibles"
            result["mode"] = "SKIP"
            return result

        hours_ago = vod_data.get("hours_ago", 9999)
        log(f"📼 {channel} → modo VOD (último stream: hace {hours_ago:.0f}h)")

        if not vod_data.get("url") and not vod_data.get("id"):
            result["error"] = f"{channel} offline, sin URL ni ID de VOD"
            result["mode"] = "SKIP"
            return result

        clipper = VODClipper(channel, vod_data)
        clips = clipper.extract_best_clips(max_clips=max_clips)

        if clips:
            result["clips"] = clips
            best = max(clips, key=lambda c: c.get("score", 0))
            result["best_clip"] = best
        else:
            result["error"] = "No se pudieron extraer clips del VOD"

    else:
        result["mode"] = "SKIP"
        result["error"] = f"{channel} no tiene stream ni VOD disponible"

    return result


def scan_all_channels(max_clips_per_channel=1):
    """
    Escanea todos los canales configurados y extrae clips.
    """
    config = load_config()
    channels = config.get("channels", [])

    if not channels:
        return {"error": "No hay canales configurados en config.json"}

    log(f"🔍 Escaneando {len(channels)} canales...")
    results = []

    for ch in channels:
        try:
            result = process_channel(ch, max_clips=max_clips_per_channel)
            results.append(result)
        except Exception as e:
            results.append({
                "channel": ch,
                "status": "ERROR",
                "mode": "SKIP",
                "clips": [],
                "error": str(e)[:200],
            })
        time.sleep(random.uniform(1, 3))

    live_count = sum(1 for r in results if r["status"] == "LIVE")
    vod_count = sum(1 for r in results if r["mode"] == "VOD")
    clips_count = sum(len(r.get("clips", [])) for r in results)
    errors = sum(1 for r in results if r.get("error"))

    log(f"📊 {live_count} LIVE, {vod_count} VOD, {clips_count} clips, {errors} errores")

    return {
        "total_channels": len(channels),
        "live": live_count,
        "vod": vod_count,
        "total_clips": clips_count,
        "errors": errors,
        "results": results,
    }


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    args = sys.argv[1:]

    if not args:
        print(json.dumps({
            "error": "Uso: python3 clip_extractor.py <canal> [--live|--vod|--best|--max-clips N|--scan]"
        }, ensure_ascii=False))
        sys.exit(1)

    # --scan: todos los canales
    if args[0] == "--scan":
        max_clips = 1
        if "--max-clips" in args:
            idx = args.index("--max-clips")
            if idx + 1 < len(args):
                max_clips = int(args[idx + 1])
        result = scan_all_channels(max_clips_per_channel=max_clips)
        print(json.dumps(result, ensure_ascii=False))
        return

    # Canal específico
    channel = args[0]
    force_mode = None
    max_clips = 3

    if "--live" in args:
        force_mode = "live"
    elif "--vod" in args:
        force_mode = "vod"

    if "--max-clips" in args:
        idx = args.index("--max-clips")
        if idx + 1 < len(args):
            max_clips = int(args[idx + 1])

    # --best → 1 mejor clip (backward compat con batch_upload.py)
    if "--best" in args:
        max_clips = 1

    result = process_channel(channel, force_mode=force_mode, max_clips=max_clips)

    # Output compatible con batch_upload.py
    output = {
        "channel": result["channel"],
        "status": result["status"],
        "mode": result["mode"],
        "clips_count": len(result.get("clips", [])),
        "clips": result.get("clips", []),
        "error": result.get("error"),
    }

    # Poner path del best_clip en raíz para compat
    if result.get("best_clip"):
        output["path"] = result["best_clip"]["path"]
        output["title"] = result["best_clip"].get("title", "")
        output["score"] = result["best_clip"].get("score", 0)
        output["source"] = result["best_clip"].get("source", "")
        output["size_mb"] = result["best_clip"].get("size_mb", 0)

    print(json.dumps(output, ensure_ascii=False))


if __name__ == "__main__":
    main()
