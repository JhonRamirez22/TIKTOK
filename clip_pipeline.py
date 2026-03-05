#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════╗
║           Fresh Peak Detector + Auto Clip Pipeline v1.0            ║
║   Monitoreo real-time de chat via WebSocket + VOD audio peaks      ║
║   Detectar → Capturar → Procesar → Subir → BORRAR del disco       ║
║   Optimizado para bajo CPU — nada de recalentamiento               ║
╚══════════════════════════════════════════════════════════════════════╝

ARQUITECTURA:
  ┌─────────────────┐     ┌──────────────┐     ┌──────────────┐
  │  ChatMonitor    │────▶│ FreshPeak    │────▶│ ClipPipeline │
  │  (Pusher WS)   │     │ Detector     │     │ (Cola FIFO)  │
  └─────────────────┘     └──────────────┘     └──────┬───────┘
                                                       │
  ┌─────────────────┐                           ┌──────▼───────┐
  │  VODPeakDetect  │──────────────────────────▶│  Capture →   │
  │  (Audio scan)   │                           │  Process →   │
  └─────────────────┘                           │  Upload →    │
                                                │  DELETE      │
                                                └──────────────┘

REGLAS:
  - Score RELATIVO al baseline del stream (no absoluto)
  - Chat score * 0.7 + Viewer score * 0.3
  - Clip cuando score >= 5, cooldown 90s entre clips
  - 55s de duración, -c copy cuando posible
  - SIEMPRE borrar clips después de subir (éxito o fallo)
  - Max 2 reintentos de upload
  - Un solo ffmpeg a la vez (proteger CPU)

USO:
  python3 clip_pipeline.py                      → monitorear todos los canales
  python3 clip_pipeline.py --channels lonche    → solo un canal
  python3 clip_pipeline.py --vod-only           → solo escanear VODs
  python3 clip_pipeline.py --live-only          → solo monitorear live
  python3 clip_pipeline.py --dry-run            → detectar sin capturar

DEPENDENCIAS:
  pip install websocket-client curl_cffi streamlink
"""

import json
import sys
import os
import re
import time
import random
import signal
import subprocess
import shutil
import threading
import queue
from datetime import datetime, timezone, timedelta
from collections import deque, Counter

try:
    import websocket as ws_lib
except ImportError:
    print("[pipeline] ❌ websocket-client no instalado. pip install websocket-client", file=sys.stderr)
    sys.exit(1)

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("[pipeline] ❌ curl_cffi no instalado. pip install curl_cffi", file=sys.stderr)
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
PYTHON = sys.executable

# Kick API
API_BASE = "https://kick.com/api/v1"
API_V2 = "https://kick.com/api/v2"

# Pusher WebSocket para chat real-time
PUSHER_KEY = "32cbd69e4b950bf97679"
PUSHER_WS = f"wss://ws-us2.pusher.com/app/{PUSHER_KEY}?protocol=7&client=js&version=8.4.0&flash=false"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
]

# ─── Peak Detection ────
MIN_PEAK_SCORE = 5          # Score mínimo para clipear
PEAK_COOLDOWN_SEC = 90      # Cooldown entre clips del mismo canal
CLIP_DURATION_SEC = 55      # Duración del clip
MAX_UPLOAD_RETRIES = 2      # Reintentos de upload
CHAT_WINDOW_SEC = 8         # Ventana de análisis de chat (segundos)
BASELINE_WINDOW_SEC = 120   # Ventana para calcular baseline (2 min)

# ─── CPU Optimization ────
MAX_CONCURRENT_FFMPEG = 1   # Solo 1 ffmpeg a la vez
FFMPEG_PRESET = "ultrafast" # Mínimo uso de CPU
MONITOR_SLEEP_SEC = 1.0     # Sleep del loop de chat
VOD_SEGMENT_SEC = 30        # Segmentos de VOD a analizar
VOD_SCAN_INTERVAL_SEC = 600 # Escanear VODs cada 10 min
CHANNEL_CHECK_INTERVAL = 60 # Verificar estado canales cada 60s

# ─── Directorios ────
CLIPS_DIR = os.path.join(SCRIPT_DIR, "clips", "pipeline")
PROCESSED_DIR = os.path.join(SCRIPT_DIR, "processed")
BUFFER_DIR = os.path.join(SCRIPT_DIR, "buffer")
LOGS_DIR = os.path.join(SCRIPT_DIR, "logs")

for d in [CLIPS_DIR, PROCESSED_DIR, BUFFER_DIR, LOGS_DIR]:
    os.makedirs(d, exist_ok=True)

# Emotes de hype para detectar momentos virales
HYPE_EMOTES = {
    "kekw", "pogchamp", "pog", "lulw", "omegalul", "monkas", "monkaw",
    "pogu", "poggers", "pepega", "sadge", "feelsstrongman", "feelsbadman",
    "feelsgoodman", "hypers", "pepelaugh", "clap", "ez", "catjam",
    "booba", "kreygasm", "weirdchamp", "widehardo", "ayaya",
    "lul", "xdd", "xd", "jajaja", "jajaj", "jaja",
    "wtf", "noooo", "nooo", "clip", "🔥", "😂", "💀", "🤣", "😱",
}

# ─── Estado global ────
_shutdown = threading.Event()
_ffmpeg_lock = threading.Semaphore(MAX_CONCURRENT_FFMPEG)
_stats = {
    "clips_created": 0,
    "clips_uploaded": 0,
    "clips_failed": 0,
    "clips_deleted": 0,
    "peaks_detected": 0,
    "start_time": None,
}


def log(msg, tag="pipeline"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}][{tag}] {msg}", file=sys.stderr, flush=True)


def load_config():
    try:
        with open(CONFIG_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def make_session():
    s = cffi_requests.Session(impersonate="chrome131")
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Referer": "https://kick.com/",
    })
    return s


def safe_delete(path):
    """Borrar archivo de forma segura. SIEMPRE borrar."""
    try:
        if path and os.path.exists(path):
            size_mb = os.path.getsize(path) / (1024 * 1024)
            os.remove(path)
            _stats["clips_deleted"] += 1
            log(f"🗑️ Borrado: {os.path.basename(path)} ({size_mb:.1f}MB)", "cleanup")
            return True
    except Exception as e:
        log(f"⚠️ No se pudo borrar {path}: {e}", "cleanup")
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# CHANNEL INFO — obtener datos del canal
# ═══════════════════════════════════════════════════════════════════════════════

def get_channel_info(session, channel):
    """Obtiene channel_id, chatroom_id, viewers, título, estado live."""
    try:
        resp = session.get(f"{API_BASE}/channels/{channel}", timeout=15)
        if resp.status_code != 200:
            return None

        data = resp.json()
        ls = data.get("livestream") or {}
        cr = data.get("chatroom") or {}

        return {
            "channel_id": data.get("id"),
            "chatroom_id": cr.get("id"),
            "is_live": data.get("livestream") is not None,
            "viewers": ls.get("viewer_count", 0) or ls.get("viewers", 0),
            "title": ls.get("session_title", ""),
            "slug": data.get("slug", channel),
        }
    except Exception as e:
        log(f"⚠️ Error info {channel}: {str(e)[:80]}", "api")
        return None


def get_channel_vods(session, channel):
    """Obtiene VODs recientes del canal."""
    for url in [
        f"{API_V2}/channels/{channel}/videos",
        f"{API_BASE}/channels/{channel}/videos",
    ]:
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                vods = data.get("data", data) if isinstance(data, dict) else data
                if isinstance(vods, list) and vods:
                    return vods
        except Exception:
            continue
    return []


# ═══════════════════════════════════════════════════════════════════════════════
# FRESH PEAK DETECTOR — scoring relativo al baseline del stream
# ═══════════════════════════════════════════════════════════════════════════════

class FreshPeakDetector:
    """
    Detecta picos de actividad RELATIVOS al baseline del stream.
    No usa valores absolutos — un stream chill con 10 msgs/min que sube
    a 30 msgs/min es tan "viral" como uno con 200 que sube a 600.

    Score = chat_score * 0.7 + viewer_score * 0.3
    Peak cuando score >= MIN_PEAK_SCORE (5)
    """

    def __init__(self, channel):
        self.channel = channel
        self.msgs_history = deque(maxlen=300)       # Últimos 5 min de msgs (1/sec)
        self.viewer_history = deque(maxlen=60)       # Últimos 60 viewers readings
        self.last_peak_time = 0                      # Timestamp del último peak
        self.baseline_msgs = 1.0                     # Baseline de msgs/sec
        self.baseline_viewers = 1                     # Baseline de viewers
        self.current_score = 0.0
        self.peaks_triggered = 0

    def update_chat_rate(self, msgs_in_window):
        """Agrega conteo de mensajes recibidos en la ventana actual."""
        self.msgs_history.append(msgs_in_window)
        self._recalculate_baseline_msgs()

    def update_viewers(self, viewers):
        """Actualiza conteo de viewers."""
        self.viewer_history.append(viewers)
        self._recalculate_baseline_viewers()

    def _recalculate_baseline_msgs(self):
        """
        Baseline = mediana de msgs en los últimos BASELINE_WINDOW_SEC readings.
        Usa mediana (no media) para que spikes repentinos NO inflen el baseline.
        """
        window = min(len(self.msgs_history), BASELINE_WINDOW_SEC)
        if window < 10:
            self.baseline_msgs = max(1.0, sum(self.msgs_history) / max(len(self.msgs_history), 1))
            return
        recent = sorted(list(self.msgs_history)[-window:])
        # Percentil 50 (mediana)
        mid = len(recent) // 2
        median = recent[mid] if len(recent) % 2 else (recent[mid - 1] + recent[mid]) / 2
        self.baseline_msgs = max(1.0, median)

    def _recalculate_baseline_viewers(self):
        """Baseline de viewers = promedio últimas lecturas."""
        if len(self.viewer_history) < 3:
            self.baseline_viewers = max(1, self.viewer_history[-1] if self.viewer_history else 1)
            return
        recent = list(self.viewer_history)[-30:]
        self.baseline_viewers = max(1, sum(recent) / len(recent))

    def calculate_score(self):
        """
        Calcula score actual RELATIVO al baseline.
        chat_score (0-10) * 0.7 + viewer_score (0-10) * 0.3
        """
        # ── Chat Score ──
        # Últimos CHAT_WINDOW_SEC mensajes vs baseline
        recent_window = min(len(self.msgs_history), CHAT_WINDOW_SEC)
        if recent_window < 2:
            chat_rate = 0
        else:
            recent_msgs = list(self.msgs_history)[-recent_window:]
            chat_rate = sum(recent_msgs) / len(recent_msgs)

        # Factor de multiplicación respecto al baseline
        chat_multiplier = chat_rate / self.baseline_msgs
        # 1x = 0, 2x = 5, 3x = 7.5, 4x+ = 10
        chat_score = min(10.0, max(0, (chat_multiplier - 1) * 5))

        # ── Viewer Score ──
        current_viewers = self.viewer_history[-1] if self.viewer_history else 0
        viewer_multiplier = current_viewers / self.baseline_viewers
        # 1x = 0, 1.5x = 5, 2x+ = 10
        viewer_score = min(10.0, max(0, (viewer_multiplier - 1) * 10))

        # ── Score Final ──
        self.current_score = round(chat_score * 0.7 + viewer_score * 0.3, 2)
        return self.current_score

    def should_clip(self):
        """
        ¿Deberíamos crear un clip ahora?
        Requisitos: score >= MIN_PEAK_SCORE Y cooldown expirado
        """
        score = self.calculate_score()
        now = time.time()
        cooldown_ok = (now - self.last_peak_time) >= PEAK_COOLDOWN_SEC

        if score >= MIN_PEAK_SCORE and cooldown_ok:
            self.last_peak_time = now
            self.peaks_triggered += 1
            _stats["peaks_detected"] += 1
            return True, score

        return False, score

    def get_status(self):
        return {
            "channel": self.channel,
            "score": self.current_score,
            "baseline_msgs": round(self.baseline_msgs, 2),
            "baseline_viewers": self.baseline_viewers,
            "peaks_triggered": self.peaks_triggered,
            "cooldown_remaining": max(0, int(PEAK_COOLDOWN_SEC - (time.time() - self.last_peak_time))),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# CHAT MONITOR — Pusher WebSocket en tiempo real
# ═══════════════════════════════════════════════════════════════════════════════

class ChatMonitor:
    """
    Monitorea chat de Kick via Pusher WebSocket.
    Suscribe a chatrooms.{chatroom_id}.v2 para recibir mensajes.
    Análisis cada 1 segundo para calcular rate.
    """

    def __init__(self, channel, chatroom_id, peak_detector, clip_queue):
        self.channel = channel
        self.chatroom_id = chatroom_id
        self.detector = peak_detector
        self.clip_queue = clip_queue
        self.ws = None
        self.msg_count_window = 0
        self.hype_count_window = 0
        self.last_window_time = time.time()
        self.connected = False
        self._thread = None
        self._analysis_thread = None

    def start(self):
        """Inicia monitoreo en threads separados."""
        self._thread = threading.Thread(target=self._ws_connect, daemon=True, name=f"ws-{self.channel}")
        self._thread.start()
        self._analysis_thread = threading.Thread(target=self._analysis_loop, daemon=True, name=f"analysis-{self.channel}")
        self._analysis_thread.start()
        log(f"🔌 Chat monitor iniciado para {self.channel} (room: {self.chatroom_id})", "chat")

    def stop(self):
        """Detiene el monitor."""
        self.connected = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        log(f"⏹️ Chat monitor detenido: {self.channel}", "chat")

    def _ws_connect(self):
        """Conecta al WebSocket de Pusher y suscribe al chatroom."""
        while not _shutdown.is_set():
            try:
                self.ws = ws_lib.WebSocket()
                self.ws.settimeout(30)
                self.ws.connect(PUSHER_WS)
                self.connected = True

                # Leer evento de conexión
                conn_msg = self.ws.recv()
                conn_data = json.loads(conn_msg)
                if conn_data.get("event") == "pusher:connection_established":
                    log(f"✅ WebSocket conectado: {self.channel}", "chat")

                # Suscribir al chatroom
                subscribe_msg = json.dumps({
                    "event": "pusher:subscribe",
                    "data": {"channel": f"chatrooms.{self.chatroom_id}.v2"}
                })
                self.ws.send(subscribe_msg)

                # Loop de recepción
                while not _shutdown.is_set() and self.connected:
                    try:
                        raw = self.ws.recv()
                        if not raw:
                            continue
                        self._process_ws_message(raw)
                    except ws_lib.WebSocketTimeoutException:
                        # Enviar ping para mantener conexión viva
                        try:
                            self.ws.send(json.dumps({"event": "pusher:ping", "data": {}}))
                        except Exception:
                            break
                    except ws_lib.WebSocketConnectionClosedException:
                        break
                    except Exception:
                        break

            except Exception as e:
                log(f"⚠️ WS error {self.channel}: {str(e)[:60]}", "chat")

            self.connected = False
            if not _shutdown.is_set():
                wait = random.uniform(3, 8)
                log(f"🔄 Reconectando {self.channel} en {wait:.0f}s...", "chat")
                _shutdown.wait(wait)

    def _process_ws_message(self, raw):
        """Procesa un mensaje del WebSocket de Pusher."""
        try:
            msg = json.loads(raw)
            event = msg.get("event", "")

            if event == "App\\Events\\ChatMessageEvent":
                data = msg.get("data")
                if isinstance(data, str):
                    data = json.loads(data)

                content = data.get("content", "")
                self.msg_count_window += 1

                # Detectar emotes de hype
                words = content.lower().split()
                for word in words:
                    clean = word.strip(".,!?¿¡;:()[]{}\"'")
                    if clean in HYPE_EMOTES:
                        self.hype_count_window += 1
                        break

            elif event == "pusher:pong":
                pass  # Keep-alive OK

        except Exception:
            pass

    def _analysis_loop(self):
        """Loop de análisis cada MONITOR_SLEEP_SEC. Calcula score y encola clips."""
        while not _shutdown.is_set():
            _shutdown.wait(MONITOR_SLEEP_SEC)
            if _shutdown.is_set():
                break

            now = time.time()
            elapsed = now - self.last_window_time

            if elapsed >= 1.0:
                # Enviar conteos al detector
                msgs_this_sec = self.msg_count_window / max(elapsed, 0.1)
                self.detector.update_chat_rate(msgs_this_sec)

                # Bonus por hype emotes: cada emote vale como 2 msgs extra
                if self.hype_count_window > 0:
                    hype_bonus = self.hype_count_window * 2
                    self.detector.update_chat_rate(msgs_this_sec + hype_bonus)

                # Reset ventana
                self.msg_count_window = 0
                self.hype_count_window = 0
                self.last_window_time = now

                # ¿Deberíamos clipear?
                should, score = self.detector.should_clip()
                if should:
                    log(f"🔥 ¡PEAK DETECTADO! {self.channel} score={score:.1f}", "peak")
                    self.clip_queue.put({
                        "channel": self.channel,
                        "score": score,
                        "type": "LIVE",
                        "timestamp": time.time(),
                        "reason": f"chat_peak_score_{score:.1f}",
                    })


# ═══════════════════════════════════════════════════════════════════════════════
# VOD PEAK DETECTOR — escaneo de audio para canales offline
# ═══════════════════════════════════════════════════════════════════════════════

class VODPeakDetector:
    """
    Escanea VODs de canales offline por segmentos de audio.
    Detecta momentos de alto volumen (gritos, reacciones).
    Usa volumedetect por segmentos de VOD_SEGMENT_SEC.
    Optimizado: un segmento a la vez, sleep entre segmentos.
    """

    def __init__(self, clip_queue):
        self.clip_queue = clip_queue
        self.scanned_vods = set()  # VOD IDs ya escaneados
        self._thread = None

    def start(self, channels):
        """Inicia escaneo periódico de VODs en background."""
        self._thread = threading.Thread(
            target=self._scan_loop, args=(channels,),
            daemon=True, name="vod-scanner"
        )
        self._thread.start()
        log("📼 VOD scanner iniciado", "vod")

    def _scan_loop(self, channels):
        """Escanea VODs periódicamente."""
        while not _shutdown.is_set():
            session = make_session()
            for channel in channels:
                if _shutdown.is_set():
                    break

                try:
                    info = get_channel_info(session, channel)
                    if not info or info.get("is_live"):
                        continue  # Solo offline

                    vods = get_channel_vods(session, channel)
                    if not vods:
                        continue

                    vod = vods[0]  # Más reciente
                    vod_id = vod.get("id")

                    if vod_id in self.scanned_vods:
                        continue

                    # ¿VOD reciente? (últimas 48h)
                    vod_date = vod.get("created_at", "") or vod.get("start_time", "")
                    try:
                        dt = datetime.fromisoformat(vod_date.replace("Z", "+00:00"))
                        hours_ago = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
                        if hours_ago > 48:
                            self.scanned_vods.add(vod_id)
                            continue
                    except Exception:
                        pass

                    log(f"📼 Escaneando VOD de {channel} (id: {vod_id})", "vod")
                    self._scan_vod(channel, vod)
                    self.scanned_vods.add(vod_id)

                except Exception as e:
                    log(f"⚠️ Error VOD scan {channel}: {str(e)[:60]}", "vod")

                # Sleep entre canales para no recalentar
                _shutdown.wait(random.uniform(2, 5))

            try:
                session.close()
            except Exception:
                pass

            # Esperar antes del siguiente ciclo
            log(f"💤 VOD scanner durmiendo {VOD_SCAN_INTERVAL_SEC // 60}min...", "vod")
            _shutdown.wait(VOD_SCAN_INTERVAL_SEC)

    def _scan_vod(self, channel, vod_data):
        """Escanea un VOD específico buscando picos de audio."""
        vod_url = vod_data.get("source", "") or vod_data.get("video", {}).get("url", "")
        vod_id = vod_data.get("id")
        duration = vod_data.get("duration", 0) or vod_data.get("length", 0)

        # Si no hay URL directa, intentar con streamlink
        if not vod_url or ".m3u8" not in vod_url:
            vod_url = self._get_vod_url_streamlink(channel, vod_id)

        if not vod_url:
            log(f"❌ Sin URL para VOD {vod_id} de {channel}", "vod")
            return

        # Obtener duración si no la tenemos
        if not duration or duration <= 0:
            duration = self._get_duration(vod_url)
        if duration <= 0:
            duration = 7200  # Default 2h

        # Escanear por segmentos
        segments = []
        offset = 60  # Saltar primer minuto (intro)

        while offset < duration - 60 and not _shutdown.is_set():
            seg_dur = min(VOD_SEGMENT_SEC, duration - offset)
            if seg_dur < 10:
                break

            with _ffmpeg_lock:
                result = self._analyze_segment(vod_url, offset, seg_dur)

            if result:
                segments.append(result)

            offset += VOD_SEGMENT_SEC
            # Sleep entre segmentos para CPU
            _shutdown.wait(0.5)

        if not segments:
            return

        # Ordenar por intensidad y filtrar top peaks
        segments.sort(key=lambda s: s["intensity"], reverse=True)

        # Filtrar solapados
        filtered = []
        for seg in segments:
            if not any(abs(seg["timestamp"] - p["timestamp"]) < PEAK_COOLDOWN_SEC for p in filtered):
                filtered.append(seg)

        # Encolar los top 3 peaks
        for peak in filtered[:3]:
            if peak["intensity"] > 15:  # Threshold mínimo
                log(f"📼 VOD peak: {channel} t={int(peak['timestamp'])}s intensity={peak['intensity']:.1f}", "vod")
                self.clip_queue.put({
                    "channel": channel,
                    "score": min(10, peak["intensity"] / 5),
                    "type": "VOD",
                    "timestamp": peak["timestamp"],
                    "vod_url": vod_url,
                    "vod_title": vod_data.get("session_title", "") or vod_data.get("title", ""),
                    "reason": f"vod_audio_peak_{peak['intensity']:.0f}",
                })

    def _analyze_segment(self, vod_url, offset, duration):
        """Analiza un segmento de audio con volumedetect."""
        try:
            result = subprocess.run(
                [
                    "ffmpeg", "-ss", str(offset), "-t", str(duration),
                    "-i", vod_url,
                    "-af", "volumedetect",
                    "-f", "null", "-",
                ],
                capture_output=True, text=True, timeout=30
            )

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

            intensity = max(0, mean_vol + 50)  # -50dB = 0, -20dB = 30
            return {
                "timestamp": offset + duration / 2,
                "start_sec": offset,
                "mean_vol": mean_vol,
                "max_vol": max_vol,
                "intensity": round(intensity, 2),
            }
        except Exception:
            return None

    def _get_vod_url_streamlink(self, channel, vod_id):
        """Obtiene URL HLS del VOD via streamlink."""
        if not shutil.which("streamlink") or not vod_id:
            return None
        for tmpl in [
            f"https://kick.com/{channel}/videos/{vod_id}",
            f"https://kick.com/video/{vod_id}",
        ]:
            try:
                r = subprocess.run(
                    ["streamlink", tmpl, "best", "--stream-url"],
                    capture_output=True, text=True, timeout=30
                )
                url = r.stdout.strip()
                if url and "http" in url:
                    return url
            except Exception:
                continue
        return None

    def _get_duration(self, url):
        """Obtiene duración de un video/stream."""
        try:
            r = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "json", "-i", url],
                capture_output=True, text=True, timeout=30
            )
            return float(json.loads(r.stdout).get("format", {}).get("duration", 0))
        except Exception:
            return 0


# ═══════════════════════════════════════════════════════════════════════════════
# CLIP PIPELINE — cola de trabajo: capturar → procesar → subir → borrar
# ═══════════════════════════════════════════════════════════════════════════════

class ClipPipeline:
    """
    Procesa clips en cola FIFO:
      1. Capturar clip (ffmpeg con -c copy o ultrafast)
      2. Procesar a 9:16 (video_processor.py --no-subs)
      3. Subir a TikTok (tiktok_uploader.py)
      4. BORRAR del disco (SIEMPRE, éxito o fallo)

    Un solo worker para proteger CPU.
    """

    def __init__(self, clip_queue):
        self.clip_queue = clip_queue
        self._thread = None
        self._active_files = []  # Archivos a borrar al final

    def start(self):
        """Inicia worker de pipeline."""
        self._thread = threading.Thread(target=self._worker, daemon=True, name="pipeline-worker")
        self._thread.start()
        log("⚙️ Clip pipeline worker iniciado", "pipeline")

    def _worker(self):
        """Worker principal: procesa items de la cola."""
        while not _shutdown.is_set():
            try:
                item = self.clip_queue.get(timeout=5)
            except queue.Empty:
                continue

            if item is None:
                break

            channel = item.get("channel", "?")
            clip_type = item.get("type", "?")
            score = item.get("score", 0)

            log(f"📋 Procesando: {channel} ({clip_type}, score={score:.1f})", "pipeline")

            raw_clip = None
            processed_clip = None

            try:
                # ─── Paso 1: Capturar ─────
                raw_clip = self._capture(item)
                if not raw_clip:
                    log(f"❌ Captura falló: {channel}", "pipeline")
                    continue

                self._active_files.append(raw_clip)
                size_mb = os.path.getsize(raw_clip) / (1024 * 1024)
                log(f"✅ Capturado: {os.path.basename(raw_clip)} ({size_mb:.1f}MB)", "pipeline")
                _stats["clips_created"] += 1

                # ─── Paso 2: Procesar a 9:16 ─────
                processed_clip = self._process(raw_clip, channel)
                if processed_clip and processed_clip != raw_clip:
                    self._active_files.append(processed_clip)

                upload_path = processed_clip or raw_clip

                # ─── Paso 3: Subir a TikTok ─────
                success = self._upload(upload_path, channel, item)
                if success:
                    _stats["clips_uploaded"] += 1
                    log(f"🎉 ¡SUBIDO! {channel} → TikTok", "pipeline")
                else:
                    _stats["clips_failed"] += 1
                    log(f"❌ Upload falló: {channel}", "pipeline")

            except Exception as e:
                log(f"💥 Error pipeline {channel}: {str(e)[:100]}", "pipeline")
                _stats["clips_failed"] += 1

            finally:
                # ─── Paso 4: BORRAR TODO (SIEMPRE) ─────
                for f in self._active_files:
                    safe_delete(f)
                self._active_files.clear()

            # Sleep entre clips para CPU
            _shutdown.wait(3)

    def _capture(self, item):
        """Captura clip según tipo (LIVE o VOD)."""
        channel = item["channel"]
        clip_type = item.get("type", "LIVE")
        ts = int(time.time())

        if clip_type == "LIVE":
            return self._capture_live(channel, ts)
        elif clip_type == "VOD":
            return self._capture_vod(channel, item, ts)
        return None

    def _capture_live(self, channel, ts):
        """Captura del stream en vivo con -c copy (sin re-encoding = bajo CPU)."""
        # Obtener URL HLS
        stream_url = None
        if shutil.which("streamlink"):
            try:
                r = subprocess.run(
                    ["streamlink", f"https://kick.com/{channel}", "best", "--stream-url"],
                    capture_output=True, text=True, timeout=30
                )
                url = r.stdout.strip()
                if url and "http" in url:
                    stream_url = url
            except Exception:
                pass

        if not stream_url:
            log(f"❌ No stream URL para {channel}", "capture")
            return None

        output = os.path.join(CLIPS_DIR, f"{channel}_live_{ts}.mp4")

        log(f"📹 Grabando {CLIP_DURATION_SEC}s live de {channel} (-c copy)...", "capture")

        with _ffmpeg_lock:
            try:
                subprocess.run(
                    [
                        "ffmpeg",
                        "-i", stream_url,
                        "-t", str(CLIP_DURATION_SEC),
                        "-c", "copy",                 # SIN re-encoding = bajo CPU
                        "-avoid_negative_ts", "make_zero",
                        "-movflags", "+faststart",
                        "-y", output,
                    ],
                    capture_output=True, text=True,
                    timeout=CLIP_DURATION_SEC + 45
                )
            except subprocess.TimeoutExpired:
                pass
            except Exception as e:
                log(f"❌ Error ffmpeg live: {str(e)[:80]}", "capture")
                return None

        if os.path.exists(output) and os.path.getsize(output) > 50000:
            return output

        # Fallback: si copy falla, usar ultrafast encoding
        log(f"⚠️ -c copy falló, reintentando con ultrafast...", "capture")
        output2 = os.path.join(CLIPS_DIR, f"{channel}_live2_{ts}.mp4")
        with _ffmpeg_lock:
            try:
                subprocess.run(
                    [
                        "ffmpeg",
                        "-i", stream_url,
                        "-t", str(CLIP_DURATION_SEC),
                        "-c:v", "libx264", "-preset", FFMPEG_PRESET,
                        "-c:a", "aac", "-b:a", "128k",
                        "-y", output2,
                    ],
                    capture_output=True, text=True,
                    timeout=CLIP_DURATION_SEC + 60
                )
            except Exception:
                return None

        if os.path.exists(output2) and os.path.getsize(output2) > 50000:
            # Limpiar el fallido
            safe_delete(output)
            return output2

        return None

    def _capture_vod(self, channel, item, ts):
        """Corta clip del VOD en el timestamp del pico."""
        vod_url = item.get("vod_url")
        peak_ts = item.get("timestamp", 60)

        if not vod_url:
            log(f"❌ Sin URL de VOD para {channel}", "capture")
            return None

        start = max(0, peak_ts - 5)  # 5s antes del pico
        output = os.path.join(CLIPS_DIR, f"{channel}_vod_{int(peak_ts)}_{ts}.mp4")

        log(f"✂️ Cortando VOD de {channel} t={int(peak_ts)}s...", "capture")

        with _ffmpeg_lock:
            try:
                subprocess.run(
                    [
                        "ffmpeg",
                        "-ss", str(start),
                        "-i", vod_url,
                        "-t", str(CLIP_DURATION_SEC),
                        "-c:v", "libx264", "-preset", FFMPEG_PRESET,
                        "-c:a", "aac", "-b:a", "128k",
                        "-avoid_negative_ts", "make_zero",
                        "-movflags", "+faststart",
                        "-y", output,
                    ],
                    capture_output=True, text=True, timeout=180
                )
            except subprocess.TimeoutExpired:
                pass
            except Exception as e:
                log(f"❌ Error ffmpeg VOD: {str(e)[:80]}", "capture")
                return None

        if os.path.exists(output) and os.path.getsize(output) > 50000:
            return output
        return None

    def _process(self, clip_path, channel):
        """Procesa clip a 9:16 con video_processor.py --no-subs."""
        script = os.path.join(SCRIPT_DIR, "video_processor.py")
        if not os.path.exists(script):
            log("⚠️ video_processor.py no encontrado, usando clip raw", "process")
            return clip_path

        try:
            result = subprocess.run(
                [PYTHON, script, clip_path, channel, "--no-subs"],
                capture_output=True, text=True, timeout=180, cwd=SCRIPT_DIR
            )

            stdout = result.stdout.strip()
            for line in reversed(stdout.split("\n")):
                line = line.strip()
                if line.startswith("{"):
                    data = json.loads(line)
                    path = data.get("path")
                    if path and os.path.exists(path):
                        return path

        except subprocess.TimeoutExpired:
            log("⚠️ Timeout procesando video, usando clip raw", "process")
        except Exception as e:
            log(f"⚠️ Error procesando: {str(e)[:80]}", "process")

        return clip_path

    def _upload(self, clip_path, channel, item):
        """Sube clip a TikTok con reintentos. Max MAX_UPLOAD_RETRIES intentos."""
        script = os.path.join(SCRIPT_DIR, "tiktok_uploader.py")
        if not os.path.exists(script):
            log("❌ tiktok_uploader.py no encontrado", "upload")
            return False

        title_hint = item.get("vod_title", "")

        for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
            try:
                log(f"📤 Subiendo {channel} intento {attempt}/{MAX_UPLOAD_RETRIES}...", "upload")

                result = subprocess.run(
                    [PYTHON, script, clip_path, channel, title_hint],
                    capture_output=True, text=True, timeout=300, cwd=SCRIPT_DIR
                )

                stdout = result.stdout.strip()
                for line in reversed(stdout.split("\n")):
                    line = line.strip()
                    if line.startswith("{"):
                        data = json.loads(line)
                        if data.get("success"):
                            return True
                        if data.get("needs_manual_login"):
                            log("🔑 ¡Login requerido! Ejecuta: python3 tiktok_uploader.py --login", "upload")
                            return False

            except subprocess.TimeoutExpired:
                log(f"⚠️ Timeout upload intento {attempt}", "upload")
            except Exception as e:
                log(f"⚠️ Error upload intento {attempt}: {str(e)[:60]}", "upload")

            if attempt < MAX_UPLOAD_RETRIES:
                wait = 10 * attempt
                log(f"⏳ Reintentando en {wait}s...", "upload")
                _shutdown.wait(wait)

        return False


# ═══════════════════════════════════════════════════════════════════════════════
# VIEWER UPDATER — actualiza viewers periódicamente sin sobrecargar
# ═══════════════════════════════════════════════════════════════════════════════

class ViewerUpdater:
    """Actualiza viewers de canales live periódicamente."""

    def __init__(self, detectors):
        self.detectors = detectors  # {channel: FreshPeakDetector}
        self._thread = None

    def start(self, channels):
        self._thread = threading.Thread(
            target=self._loop, args=(channels,),
            daemon=True, name="viewer-updater"
        )
        self._thread.start()

    def _loop(self, channels):
        while not _shutdown.is_set():
            session = make_session()
            for ch in channels:
                if _shutdown.is_set():
                    break
                if ch not in self.detectors:
                    continue
                try:
                    info = get_channel_info(session, ch)
                    if info and info.get("is_live"):
                        self.detectors[ch].update_viewers(info.get("viewers", 0))
                except Exception:
                    pass
                _shutdown.wait(1)

            try:
                session.close()
            except Exception:
                pass

            _shutdown.wait(CHANNEL_CHECK_INTERVAL)


# ═══════════════════════════════════════════════════════════════════════════════
# STATUS DISPLAY — muestra estado del pipeline
# ═══════════════════════════════════════════════════════════════════════════════

def status_display(detectors, monitors, clip_queue):
    """Thread que muestra estado periódicamente."""
    while not _shutdown.is_set():
        _shutdown.wait(30)
        if _shutdown.is_set():
            break

        uptime = ""
        if _stats["start_time"]:
            elapsed = time.time() - _stats["start_time"]
            h, m = int(elapsed // 3600), int((elapsed % 3600) // 60)
            uptime = f"{h}h{m:02d}m"

        log(f"{'─' * 55}", "status")
        log(f"⏱️ Uptime: {uptime} | Cola: {clip_queue.qsize()} | "
            f"Creados: {_stats['clips_created']} | Subidos: {_stats['clips_uploaded']} | "
            f"Fallos: {_stats['clips_failed']} | Borrados: {_stats['clips_deleted']}", "status")

        for ch, det in detectors.items():
            status = det.get_status()
            ws_ok = "🟢" if ch in monitors and monitors[ch].connected else "🔴"
            log(f"  {ws_ok} {ch}: score={status['score']:.1f} | "
                f"baseline_msgs={status['baseline_msgs']:.1f} | "
                f"peaks={status['peaks_triggered']} | "
                f"cd={status['cooldown_remaining']}s", "status")
        log(f"{'─' * 55}", "status")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN — orquestador principal
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    args = sys.argv[1:]

    # Parsear argumentos
    channels = None
    vod_only = "--vod-only" in args
    live_only = "--live-only" in args
    dry_run = "--dry-run" in args

    if "--channels" in args:
        idx = args.index("--channels")
        channels = []
        for a in args[idx + 1:]:
            if a.startswith("--"):
                break
            channels.append(a)

    # Cargar config
    config = load_config()
    if not channels:
        channels = config.get("channels", [])

    if not channels:
        log("❌ No hay canales. Usa --channels o configura config.json", "main")
        sys.exit(1)

    # Señal de apagado
    def signal_handler(sig, frame):
        log("\n🛑 Apagando pipeline...", "main")
        _shutdown.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Banner
    log("╔══════════════════════════════════════════════════════╗", "main")
    log("║    Fresh Peak Detector + Auto Clip Pipeline v1.0    ║", "main")
    log("║    Monitoreo → Detectar → Clip → Upload → DELETE    ║", "main")
    log(f"║    Canales: {', '.join(channels):<40}║", "main")
    log(f"║    Modo: {'VOD' if vod_only else 'LIVE' if live_only else 'AUTO':<46}║", "main")
    log(f"║    Dry-run: {'Sí' if dry_run else 'No':<43}║", "main")
    log("╚══════════════════════════════════════════════════════╝", "main")

    _stats["start_time"] = time.time()

    # Cola compartida para clips
    clip_queue = queue.Queue(maxsize=20)
    if dry_run:
        clip_queue = _DryRunQueue()

    # Detectar estado de cada canal
    session = make_session()
    live_channels = []
    offline_channels = []

    for ch in channels:
        info = get_channel_info(session, ch)
        if not info:
            log(f"⚠️ No se pudo obtener info de {ch}", "main")
            offline_channels.append(ch)
            continue

        if info.get("is_live"):
            live_channels.append((ch, info))
            log(f"🔴 {ch} → LIVE ({info.get('viewers', 0)} viewers)", "main")
        else:
            offline_channels.append(ch)
            log(f"⚫ {ch} → OFFLINE", "main")

        _shutdown.wait(0.5)

    session.close()

    # Crear detectores y monitores para canales live
    detectors = {}   # channel → FreshPeakDetector
    monitors = {}    # channel → ChatMonitor

    if not vod_only:
        for ch, info in live_channels:
            chatroom_id = info.get("chatroom_id")
            if not chatroom_id:
                log(f"⚠️ Sin chatroom_id para {ch}", "main")
                continue

            detector = FreshPeakDetector(ch)
            detector.update_viewers(info.get("viewers", 0))
            detectors[ch] = detector

            monitor = ChatMonitor(ch, chatroom_id, detector, clip_queue)
            monitor.start()
            monitors[ch] = monitor

            _shutdown.wait(1)  # Delay entre conexiones WS

    # Pipeline worker (captura → procesa → sube → borra)
    pipeline = ClipPipeline(clip_queue)
    if not dry_run:
        pipeline.start()

    # VOD scanner para offline channels
    vod_detector = None
    if not live_only and offline_channels:
        vod_detector = VODPeakDetector(clip_queue)
        vod_detector.start(offline_channels)

    # Viewer updater
    viewer_updater = None
    if detectors:
        viewer_updater = ViewerUpdater(detectors)
        viewer_updater.start([ch for ch, _ in live_channels])

    # Status display
    status_thread = threading.Thread(
        target=status_display, args=(detectors, monitors, clip_queue),
        daemon=True, name="status"
    )
    status_thread.start()

    # Re-check periódico: canales offline → live y viceversa
    log("🔄 Pipeline activo. Ctrl+C para detener.", "main")

    while not _shutdown.is_set():
        _shutdown.wait(CHANNEL_CHECK_INTERVAL)
        if _shutdown.is_set():
            break

        # Re-check canales
        try:
            session = make_session()
            for ch in channels:
                if _shutdown.is_set():
                    break

                info = get_channel_info(session, ch)
                if not info:
                    continue

                is_live_now = info.get("is_live", False)
                was_monitoring = ch in monitors

                # Canal se puso live → iniciar monitor
                if is_live_now and not was_monitoring and not vod_only:
                    chatroom_id = info.get("chatroom_id")
                    if chatroom_id:
                        log(f"🔴 {ch} ahora está LIVE → iniciando monitor", "main")
                        detector = FreshPeakDetector(ch)
                        detector.update_viewers(info.get("viewers", 0))
                        detectors[ch] = detector

                        monitor = ChatMonitor(ch, chatroom_id, detector, clip_queue)
                        monitor.start()
                        monitors[ch] = monitor

                # Canal se fue offline → detener monitor
                elif not is_live_now and was_monitoring:
                    log(f"⚫ {ch} se fue OFFLINE → deteniendo monitor", "main")
                    monitors[ch].stop()
                    del monitors[ch]
                    if ch in detectors:
                        del detectors[ch]

                _shutdown.wait(1)

            session.close()
        except Exception as e:
            log(f"⚠️ Error re-check: {str(e)[:80]}", "main")

    # Cleanup
    log("🧹 Limpiando...", "main")
    for ch, mon in monitors.items():
        mon.stop()

    log(f"📊 RESUMEN FINAL:", "main")
    log(f"   Peaks detectados: {_stats['peaks_detected']}", "main")
    log(f"   Clips creados: {_stats['clips_created']}", "main")
    log(f"   Clips subidos: {_stats['clips_uploaded']}", "main")
    log(f"   Clips fallidos: {_stats['clips_failed']}", "main")
    log(f"   Archivos borrados: {_stats['clips_deleted']}", "main")
    log("✅ Pipeline detenido.", "main")


class _DryRunQueue:
    """Cola falsa para dry-run: solo loguea, no procesa."""

    def put(self, item):
        log(f"🔍 [DRY-RUN] Clip detectado: {item.get('channel')} "
            f"type={item.get('type')} score={item.get('score', 0):.1f} "
            f"reason={item.get('reason', '?')}", "dry-run")

    def get(self, timeout=5):
        raise queue.Empty

    def qsize(self):
        return 0


if __name__ == "__main__":
    main()
