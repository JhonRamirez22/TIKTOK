#!/usr/bin/env python3
"""
KickClipBot — viral_detector.py
═══════════════════════════════════════════════════════════════
Detecta momentos virales en streams de Kick en tiempo real.

Monitorea chat via WebSocket de Kick (Pusher) y calcula un
score de viralidad (1-10) basado en:
  - Velocidad del chat: msgs/min (40%)
  - Pico de viewers en el momento (30%)
  - Repetición de emotes de hype (30%)

Solo señala momentos con score >= umbral (default 7).

Uso:
    python3 viral_detector.py <canal>                → monitorea en vivo (blocking)
    python3 viral_detector.py <canal> --snapshot      → snapshot único del chat
    python3 viral_detector.py <canal> --score          → calcula score actual
    python3 viral_detector.py --history                → muestra historial de scores

Salida: JSON a stdout
"""

import json
import sys
import os
import time
import random
import threading
from datetime import datetime, timezone
from collections import deque, Counter

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print(json.dumps({"error": "curl_cffi no instalado. Ejecuta: pip install curl_cffi"}))
    sys.exit(1)

# ─── Configuración ────────────────────────────────────────────────────────────

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
SCORES_PATH = os.path.join(SCRIPT_DIR, "clips_score.json")

API_BASE   = "https://kick.com/api/v1"
API_V2     = "https://kick.com/api/v2"
PUSHER_KEY = "32cbd69e4b950bf97679"
PUSHER_WS  = f"wss://ws-us2.pusher.com/app/{PUSHER_KEY}?protocol=7&client=js&version=8.4.0&flash=false"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

# Emotes de hype — señales de momentos virales
HYPE_EMOTES = [
    "KEKW", "PogChamp", "Pog", "LULW", "OMEGALUL", "monkaS", "monkaW",
    "PogU", "POGGERS", "Pepega", "Sadge", "FeelsStrongMan", "FeelsBadMan",
    "FeelsGoodMan", "HYPERS", "PepeLaugh", "Clap", "EZ", "catJAM",
    "BOOBA", "Kreygasm", "WeirdChamp", "WideHardo", "AYAYA",
    "LUL", "xdd", "xd", "XD", "JAJAJA", "jajaja", "JAJAJ",
    "wtf", "WTF", "noooo", "NOOO", "clip", "CLIP",
    "🔥", "😂", "💀", "🤣", "😱", "🫠",
]

# Convertir a set para lookups rápidos
HYPE_EMOTES_SET  = set(e.lower() for e in HYPE_EMOTES)
HYPE_EMOTES_HIGH = {"kekw", "pogchamp", "pog", "omegalul", "lulw", "clip", "wtf", "noooo"}


def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def load_scores_history():
    try:
        with open(SCORES_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return []


def save_score_entry(entry):
    """Añade una entrada al historial de scores."""
    history = load_scores_history()
    history.append(entry)
    # Mantener últimos 500 entries
    if len(history) > 500:
        history = history[-500:]
    try:
        with open(SCORES_PATH, "w") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[viral_detector] Error guardando score: {e}", file=sys.stderr)


def create_session():
    return cffi_requests.Session(impersonate="chrome131")


# ─── Info del Canal ───────────────────────────────────────────────────────────

def get_channel_info(session, channel_name):
    """Obtiene channel_id, chatroom_id, viewers, título."""
    ua = random.choice(USER_AGENTS)
    try:
        res = session.get(
            f"{API_BASE}/channels/{channel_name}",
            headers={
                "User-Agent": ua,
                "Accept": "application/json",
                "Referer": f"https://kick.com/{channel_name}",
                "Origin": "https://kick.com",
            },
            timeout=20,
        )
        if res.status_code == 200:
            data = res.json()
            ls = data.get("livestream") or {}
            cr = data.get("chatroom") or {}
            return {
                "channel_id": data.get("id"),
                "chatroom_id": cr.get("id"),
                "is_live": data.get("livestream") is not None,
                "viewers": ls.get("viewers", 0),
                "title": ls.get("session_title", ""),
                "slug": data.get("slug", channel_name),
            }
        return {"error": f"HTTP {res.status_code}"}
    except Exception as e:
        return {"error": str(e)[:200]}


# ─── Análisis de Chat via API (snapshot) ──────────────────────────────────────

def get_chat_snapshot(session, channel_id, chatroom_id, channel_name):
    """
    Obtiene mensajes recientes del chat y analiza actividad.
    Retorna métricas de chat.
    """
    ua = random.choice(USER_AGENTS)
    messages = []

    # Intentar endpoint de mensajes
    endpoints = [
        f"{API_V2}/channels/{channel_id}/messages",
        f"https://kick.com/api/v2/channels/{channel_name}/messages",
    ]

    for url in endpoints:
        try:
            res = session.get(
                url,
                headers={
                    "User-Agent": ua,
                    "Accept": "application/json",
                    "Referer": f"https://kick.com/{channel_name}",
                    "Origin": "https://kick.com",
                },
                timeout=15,
            )
            if res.status_code == 200:
                data = res.json()
                if isinstance(data, list):
                    messages = data
                elif isinstance(data, dict):
                    messages = data.get("data", {}).get("messages", [])
                    if not messages:
                        messages = data.get("messages", [])
                if messages:
                    break
        except Exception:
            continue

    if not messages:
        return {
            "message_count": 0,
            "msgs_per_min": 0,
            "hype_emote_count": 0,
            "hype_ratio": 0,
            "top_emotes": [],
        }

    # Analizar mensajes
    total_msgs = len(messages)
    emote_counter = Counter()
    hype_count = 0

    for msg in messages:
        content = ""
        if isinstance(msg, dict):
            content = msg.get("content", msg.get("message", ""))
        elif isinstance(msg, str):
            content = msg

        content_lower = content.lower()
        words = content_lower.split()

        for word in words:
            clean = word.strip(".,!?¿¡;:()[]{}\"'")
            if clean in HYPE_EMOTES_SET:
                emote_counter[clean] += 1
                hype_count += 1

    # Estimar msgs/min (asumimos ventana de ~60s para los mensajes visibles)
    msgs_per_min = min(total_msgs, 200)

    hype_ratio = hype_count / max(total_msgs, 1)

    top_emotes = emote_counter.most_common(5)

    return {
        "message_count": total_msgs,
        "msgs_per_min": msgs_per_min,
        "hype_emote_count": hype_count,
        "hype_ratio": round(hype_ratio, 3),
        "top_emotes": [{"emote": e, "count": c} for e, c in top_emotes],
    }


# ─── Cálculo de Score Viral ──────────────────────────────────────────────────

def calculate_viral_score(chat_metrics, viewers, channel_name):
    """
    Calcula score viral del 1-10 basado en:
      - Velocidad del chat: msgs/min (40%)
      - Pico de viewers (30%)
      - Repetición de emotes de hype (30%)
    """
    config = load_config()
    viral_cfg = config.get("viral_detection", {})
    msgs_threshold = viral_cfg.get("msgs_per_min_threshold", 50)
    viewers_threshold = viral_cfg.get("viewers_threshold", 1000)
    min_score = viral_cfg.get("min_viral_score", 7)

    msgs_per_min = chat_metrics.get("msgs_per_min", 0)
    hype_count   = chat_metrics.get("hype_emote_count", 0)
    hype_ratio   = chat_metrics.get("hype_ratio", 0)

    # ── Chat Speed Score (40%) ──
    # 0 msgs/min = 0, threshold msgs/min = 10
    chat_raw = min(msgs_per_min / max(msgs_threshold, 1), 1.0) * 10
    chat_score = chat_raw * 0.4

    # ── Viewer Score (30%) ──
    # Normalizar contra threshold
    viewer_raw = min(viewers / max(viewers_threshold, 1), 1.0) * 10
    viewer_score = viewer_raw * 0.3

    # ── Hype Emote Score (30%) ──
    # Combinación de ratio + conteo absoluto
    ratio_score = min(hype_ratio / 0.3, 1.0) * 5  # 30% de msgs son emotes = perfecto
    count_score = min(hype_count / 30, 1.0) * 5     # 30 emotes en la ventana = perfecto
    hype_raw = ratio_score + count_score
    hype_score = hype_raw * 0.3

    # Score final
    total = round(chat_score + viewer_score + hype_score, 1)
    total = max(1.0, min(10.0, total))

    return {
        "score": total,
        "chat_component": round(chat_score, 2),
        "viewer_component": round(viewer_score, 2),
        "hype_component": round(hype_score, 2),
        "msgs_per_min": msgs_per_min,
        "viewers": viewers,
        "hype_emote_count": hype_count,
        "hype_ratio": round(hype_ratio, 3),
        "is_viral": total >= min_score,
        "min_score": min_score,
        "channel": channel_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ─── Score Snapshot (llamado desde Node.js) ───────────────────────────────────

def get_viral_score(channel_name):
    """
    Calcula el score viral actual de un canal.
    Función principal llamada desde el orquestador.
    """
    session = create_session()

    # Obtener info del canal
    info = get_channel_info(session, channel_name)
    if info.get("error"):
        return {
            "channel": channel_name,
            "score": 0,
            "is_viral": False,
            "error": info["error"],
        }

    if not info.get("is_live"):
        return {
            "channel": channel_name,
            "score": 0,
            "is_viral": False,
            "is_live": False,
            "error": "Canal no está en vivo",
        }

    viewers = info.get("viewers", 0)
    channel_id = info.get("channel_id")
    chatroom_id = info.get("chatroom_id")

    time.sleep(random.uniform(0.3, 0.8))

    # Obtener métricas del chat
    chat_metrics = get_chat_snapshot(session, channel_id, chatroom_id, channel_name)

    # Calcular score
    score_data = calculate_viral_score(chat_metrics, viewers, channel_name)
    score_data["title"] = info.get("title", "")
    score_data["is_live"] = True
    score_data["top_emotes"] = chat_metrics.get("top_emotes", [])

    # Guardar en historial si el score es significativo (>= 4)
    if score_data["score"] >= 4:
        save_score_entry({
            "channel": channel_name,
            "score": score_data["score"],
            "viewers": viewers,
            "msgs_per_min": chat_metrics["msgs_per_min"],
            "hype_count": chat_metrics["hype_emote_count"],
            "title": info.get("title", ""),
            "is_viral": score_data["is_viral"],
            "timestamp": score_data["timestamp"],
        })

    return score_data


def get_all_viral_scores(channels=None):
    """Calcula score viral de todos los canales proporcionados."""
    config = load_config()
    if channels is None:
        channels = config.get("channels", [])

    results = []
    for ch in channels:
        result = get_viral_score(ch)
        results.append(result)
        time.sleep(random.uniform(0.5, 1.5))

    # Ordenar por score (mayor primero)
    results.sort(key=lambda x: x.get("score", 0), reverse=True)
    return results


def get_score_history(channel=None, limit=50):
    """Retorna historial de scores, opcionalmente filtrado por canal."""
    history = load_scores_history()
    if channel:
        history = [h for h in history if h.get("channel") == channel]
    return history[-limit:]


# ─── Monitoreo Continuo via Chat Polling ──────────────────────────────────────

def monitor_chat_continuous(channel_name, duration_sec=300, interval_sec=10):
    """
    Monitorea el chat de un canal durante `duration_sec` segundos,
    tomando snapshots cada `interval_sec`.
    Retorna el pico de score viral detectado.
    """
    session = create_session()
    info = get_channel_info(session, channel_name)

    if info.get("error") or not info.get("is_live"):
        return {
            "channel": channel_name,
            "peak_score": 0,
            "is_viral": False,
            "error": info.get("error", "No en vivo"),
            "snapshots": 0,
        }

    channel_id = info["channel_id"]
    chatroom_id = info["chatroom_id"]
    viewers_base = info.get("viewers", 0)

    peak_score = 0
    peak_data = None
    snapshots = 0
    start_time = time.time()

    print(f"[viral_detector] Monitoreando {channel_name} por {duration_sec}s...", file=sys.stderr)

    while time.time() - start_time < duration_sec:
        try:
            # Refresh viewers
            fresh_info = get_channel_info(session, channel_name)
            current_viewers = fresh_info.get("viewers", viewers_base) if not fresh_info.get("error") else viewers_base

            # Snapshot del chat
            chat = get_chat_snapshot(session, channel_id, chatroom_id, channel_name)

            # Score
            score = calculate_viral_score(chat, current_viewers, channel_name)
            snapshots += 1

            if score["score"] > peak_score:
                peak_score = score["score"]
                peak_data = score
                peak_data["top_emotes"] = chat.get("top_emotes", [])

            if score["score"] >= 7:
                print(f"[viral_detector] 🔥 MOMENTO VIRAL: {channel_name} score={score['score']}", file=sys.stderr)

        except Exception as e:
            print(f"[viral_detector] Error snapshot: {str(e)[:100]}", file=sys.stderr)

        time.sleep(interval_sec)

    result = {
        "channel": channel_name,
        "peak_score": peak_score,
        "is_viral": peak_score >= 7,
        "snapshots": snapshots,
        "duration_monitored_sec": int(time.time() - start_time),
        "peak_data": peak_data,
        "title": info.get("title", ""),
    }

    # Guardar peak en historial
    if peak_data and peak_score >= 4:
        save_score_entry({
            "channel": channel_name,
            "score": peak_score,
            "viewers": peak_data.get("viewers", 0),
            "msgs_per_min": peak_data.get("msgs_per_min", 0),
            "hype_count": peak_data.get("hype_emote_count", 0),
            "title": info.get("title", ""),
            "is_viral": peak_score >= 7,
            "type": "continuous_peak",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    return result


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    if len(args) == 0:
        # Score de todos los canales
        results = get_all_viral_scores()
        print(json.dumps(results, ensure_ascii=False))
        return

    if args[0] == "--history":
        channel = args[1] if len(args) > 1 else None
        limit = int(args[2]) if len(args) > 2 else 50
        history = get_score_history(channel, limit)
        print(json.dumps(history, ensure_ascii=False))
        return

    if args[0] == "--all-scores":
        results = get_all_viral_scores()
        print(json.dumps(results, ensure_ascii=False))
        return

    channel = args[0]

    if len(args) > 1 and args[1] == "--snapshot":
        result = get_viral_score(channel)
        print(json.dumps(result, ensure_ascii=False))

    elif len(args) > 1 and args[1] == "--score":
        result = get_viral_score(channel)
        print(json.dumps(result, ensure_ascii=False))

    elif len(args) > 1 and args[1] == "--monitor":
        duration = int(args[2]) if len(args) > 2 else 300
        interval = int(args[3]) if len(args) > 3 else 10
        result = monitor_chat_continuous(channel, duration, interval)
        print(json.dumps(result, ensure_ascii=False))

    else:
        # Default: score snapshot
        result = get_viral_score(channel)
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
