#!/usr/bin/env python3
"""
KickClipBot — clip_monitor.py
═══════════════════════════════════════════════════════════════
Monitorea canales de Kick en tiempo real.
Detecta streams en vivo y momentos de alto engagement.

Uso desde Node.js (spawnSync) o directo:
    python3 clip_monitor.py                  → chequea todos los canales
    python3 clip_monitor.py lonche           → chequea solo "lonche"
    python3 clip_monitor.py --engagement lonche → chequea engagement de "lonche"

Salida: JSON a stdout
"""

import json
import sys
import time
import random
import os
from datetime import datetime

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print(json.dumps({"error": "curl_cffi no instalado. Ejecuta: pip install curl_cffi"}))
    sys.exit(1)

# ─── Configuración ────────────────────────────────────────────────────────────

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

DEFAULT_CHANNELS = ["lonche", "willito", "westcol", "lasapaaaaa", "chanty", "Juanferlamata"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
]

API_BASE = "https://kick.com/api/v1"
API_V2   = "https://kick.com/api/v2"


def load_config():
    """Carga config.json si existe."""
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def create_session():
    """Crea sesión curl_cffi con impersonate Chrome 131."""
    session = cffi_requests.Session(impersonate="chrome131")
    return session


def get_channel_info(session, channel_name):
    """
    Obtiene información completa de un canal.
    Retorna dict con: id, chatroom_id, live, viewers, title, category, etc.
    """
    ua = random.choice(USER_AGENTS)
    url = f"{API_BASE}/channels/{channel_name}"

    try:
        res = session.get(
            url,
            headers={
                "User-Agent": ua,
                "Accept": "application/json",
                "Referer": f"https://kick.com/{channel_name}",
                "Origin": "https://kick.com",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            },
            timeout=20,
        )

        if res.status_code == 200:
            data = res.json()
            livestream = data.get("livestream") or {}
            chatroom = data.get("chatroom") or {}
            category = livestream.get("categories") or [{}]
            cat_name = ""
            if isinstance(category, list) and len(category) > 0:
                cat_name = category[0].get("name", "")
            elif isinstance(category, dict):
                cat_name = category.get("name", "")

            return {
                "channel": channel_name,
                "channel_id": data.get("id"),
                "slug": data.get("slug", channel_name),
                "chatroom_id": chatroom.get("id"),
                "is_live": data.get("livestream") is not None,
                "viewers": livestream.get("viewers", 0),
                "title": livestream.get("session_title", ""),
                "category": cat_name,
                "language": livestream.get("language", ""),
                "livestream_id": livestream.get("id"),
                "playback_url": livestream.get("playback_url", ""),
                "thumbnail": livestream.get("thumbnail", {}).get("url", "") if isinstance(livestream.get("thumbnail"), dict) else "",
                "started_at": livestream.get("created_at", ""),
                "verified": data.get("verified", False),
                "followers_count": data.get("followers_count", 0),
                "profile_pic": data.get("user", {}).get("profile_pic", ""),
                "checked_at": datetime.utcnow().isoformat() + "Z",
                "error": None,
            }

        elif res.status_code == 404:
            return {
                "channel": channel_name,
                "is_live": False,
                "error": "Canal no encontrado (404)",
                "checked_at": datetime.utcnow().isoformat() + "Z",
            }
        else:
            return {
                "channel": channel_name,
                "is_live": False,
                "error": f"HTTP {res.status_code}",
                "checked_at": datetime.utcnow().isoformat() + "Z",
            }

    except Exception as e:
        return {
            "channel": channel_name,
            "is_live": False,
            "error": str(e)[:200],
            "checked_at": datetime.utcnow().isoformat() + "Z",
        }


def check_engagement(session, channel_name, channel_id, chatroom_id):
    """
    Analiza el nivel de engagement del canal.
    Retorna métricas: viewers, chat_rate, engagement_score.
    """
    config = load_config()
    monitor_cfg = config.get("monitor_settings", {})
    viewer_threshold = monitor_cfg.get("engagement_threshold_viewers", 500)
    chat_threshold = monitor_cfg.get("engagement_threshold_chat_rate", 20)

    ua = random.choice(USER_AGENTS)
    engagement = {
        "channel": channel_name,
        "viewers": 0,
        "chat_messages_sample": 0,
        "chat_rate_per_min": 0,
        "engagement_score": 0,
        "is_high_engagement": False,
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }

    # Obtener viewers actuales
    info = get_channel_info(session, channel_name)
    if info.get("error"):
        engagement["error"] = info["error"]
        return engagement

    engagement["viewers"] = info.get("viewers", 0)

    # Intentar obtener mensajes recientes del chat para estimar actividad
    if chatroom_id:
        try:
            chat_url = f"{API_V2}/channels/{channel_id}/messages"
            res = session.get(
                chat_url,
                headers={
                    "User-Agent": ua,
                    "Accept": "application/json",
                    "Referer": f"https://kick.com/{channel_name}",
                    "Origin": "https://kick.com",
                },
                timeout=15,
            )
            if res.status_code == 200:
                messages = res.json()
                if isinstance(messages, dict):
                    messages = messages.get("data", {}).get("messages", [])
                if isinstance(messages, list):
                    engagement["chat_messages_sample"] = len(messages)
                    # Estimar tasa: si obtenemos N mensajes en la ventana visible
                    # asumimos que representa ~1 minuto de actividad
                    engagement["chat_rate_per_min"] = min(len(messages), 100)
        except Exception:
            pass

    # Calcular score de engagement (0-100)
    viewer_score = min((engagement["viewers"] / max(viewer_threshold, 1)) * 50, 50)
    chat_score = min((engagement["chat_rate_per_min"] / max(chat_threshold, 1)) * 50, 50)
    engagement["engagement_score"] = round(viewer_score + chat_score, 1)
    engagement["is_high_engagement"] = (
        engagement["viewers"] >= viewer_threshold
        or engagement["chat_rate_per_min"] >= chat_threshold
        or engagement["engagement_score"] >= 60
    )

    return engagement


def check_all_channels(channels=None):
    """Chequea todos los canales y retorna estado de cada uno."""
    config = load_config()
    if channels is None:
        channels = config.get("channels", DEFAULT_CHANNELS)

    session = create_session()
    results = []

    for channel in channels:
        info = get_channel_info(session, channel)
        results.append(info)
        # Pequeño delay entre requests para no ser bloqueado
        time.sleep(random.uniform(0.5, 1.5))

    return results


def check_single_channel(channel_name):
    """Chequea un solo canal."""
    session = create_session()
    return get_channel_info(session, channel_name)


def check_channel_engagement(channel_name):
    """Chequea engagement de un canal específico."""
    session = create_session()
    info = get_channel_info(session, channel_name)
    if info.get("error") or not info.get("is_live"):
        return {
            "channel": channel_name,
            "is_live": info.get("is_live", False),
            "engagement_score": 0,
            "is_high_engagement": False,
            "error": info.get("error", "Canal no está en vivo"),
        }

    return check_engagement(
        session,
        channel_name,
        info.get("channel_id"),
        info.get("chatroom_id"),
    )


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    if len(args) == 0:
        # Chequear todos los canales
        results = check_all_channels()
        print(json.dumps(results, ensure_ascii=False))

    elif args[0] == "--engagement" and len(args) >= 2:
        # Chequear engagement de un canal
        result = check_channel_engagement(args[1])
        print(json.dumps(result, ensure_ascii=False))

    elif args[0] == "--all-engagement":
        # Chequear engagement de todos los canales en vivo
        config = load_config()
        channels = config.get("channels", DEFAULT_CHANNELS)
        session = create_session()
        results = []
        for ch in channels:
            info = get_channel_info(session, ch)
            if info.get("is_live"):
                eng = check_engagement(session, ch, info.get("channel_id"), info.get("chatroom_id"))
                eng["title"] = info.get("title", "")
                eng["playback_url"] = info.get("playback_url", "")
                results.append(eng)
            else:
                results.append({
                    "channel": ch,
                    "is_live": False,
                    "engagement_score": 0,
                    "is_high_engagement": False,
                })
            time.sleep(random.uniform(0.5, 1.5))
        print(json.dumps(results, ensure_ascii=False))

    elif args[0] == "--live-only":
        # Solo canales en vivo
        results = check_all_channels()
        live = [r for r in results if r.get("is_live")]
        print(json.dumps(live, ensure_ascii=False))

    else:
        # Canal específico
        result = check_single_channel(args[0])
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
