#!/usr/bin/env python3
"""
KickClipBot — publish_optimizer.py
═══════════════════════════════════════════════════════════════
Optimizador de publicación para TikTok.

Funcionalidades:
  - Cola inteligente con horarios peak Colombia (UTC-5)
  - Máximo 5 clips/día, 2 por canal/día
  - Mínimo 2 horas entre publicaciones
  - No publicar entre 1am-6am
  - Generador de metadata: títulos rotativos, hashtags por canal,
    trending hashtags, CTAs
  - Scraping de trending hashtags de TikTok explore

Uso:
    python3 publish_optimizer.py --can-publish            → ¿se puede publicar ahora?
    python3 publish_optimizer.py --next-slot               → próximo horario disponible
    python3 publish_optimizer.py --metadata <canal> [titulo] → genera metadata
    python3 publish_optimizer.py --queue                    → muestra cola actual
    python3 publish_optimizer.py --record <canal>           → registra una publicación
    python3 publish_optimizer.py --stats                    → estadísticas del día

Salida: JSON a stdout
"""

import json
import sys
import os
import time
import random
from datetime import datetime, timedelta, timezone

# ─── Configuración ────────────────────────────────────────────────────────────

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
PUBLISH_LOG = os.path.join(SCRIPT_DIR, "publish_log.json")

# Zona horaria Colombia UTC-5
COL_UTC_OFFSET = -5
COL_TZ = timezone(timedelta(hours=COL_UTC_OFFSET))

# Horarios peak Colombia
PEAK_HOURS = [
    {"hour": 7,  "minute": 0,  "priority": "medium",  "label": "Mañana temprano"},
    {"hour": 12, "minute": 0,  "priority": "medium",  "label": "Mediodía"},
    {"hour": 19, "minute": 0,  "priority": "high",    "label": "Prime time"},
    {"hour": 20, "minute": 30, "priority": "high",    "label": "Prime time nocturno"},
    {"hour": 22, "minute": 0,  "priority": "highest", "label": "Peak máximo"},
]

# Hashtags
HASHTAGS_BASE = [
    "#kick", "#clips", "#viral", "#streamer",
    "#colombia", "#parati", "#fyp", "#foryou",
]

HASHTAGS_POR_CANAL = {
    "lonche":        ["#lonche", "#lonchestream", "#loncheenKick"],
    "willito":       ["#willito", "#willitovc", "#willitostream"],
    "westcol":       ["#westcol", "#westcolclips", "#westcolenvivo"],
    "lasapaaaaa":    ["#lasapa", "#lasapaaaaa", "#lasapastream"],
    "chanty":        ["#chanty", "#chantystream", "#chantyenkick"],
    "juanferlamata": ["#juanferlamata", "#juanfer", "#juanferstream"],
}

# Templates de descripción rotativas (CTA incluido)
DESCRIPTION_TEMPLATES = [
    "🔥 Momento ÉPICO de {canal} en Kick! Sígueme para más clips 🎬",
    "😂 NO VAS A CREER lo que pasó en el stream de {canal}! Follow para más 🔴",
    "💀 {canal} en su MEJOR momento! Dale follow para clips diarios 🎮",
    "🫠 Esto pasó EN VIVO en el stream de {canal}! Sígueme para no perderte nada 🚀",
    "🔴 CLIP VIRAL de {canal} en Kick! Like + Follow = más clips 🔥",
]

# Template con CTA de referido (1 de cada 5)
REFERRAL_TEMPLATE = "🔥 {canal} en Kick! Regístrate y gana 👉 {referral_link} | Sígueme para más clips 🎬"


def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def load_publish_log():
    try:
        with open(PUBLISH_LOG, "r") as f:
            return json.load(f)
    except Exception:
        return {"publications": [], "daily_counts": {}}


def save_publish_log(log):
    try:
        with open(PUBLISH_LOG, "w") as f:
            json.dump(log, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[publish_optimizer] Error guardando log: {e}", file=sys.stderr)


def now_colombia():
    """Retorna datetime actual en zona horaria Colombia."""
    return datetime.now(COL_TZ)


def today_str():
    return now_colombia().strftime("%Y-%m-%d")


# ─── Registro de Publicaciones ────────────────────────────────────────────────

def record_publication(channel_name, clip_title="", viral_score=0):
    """Registra una publicación realizada."""
    log = load_publish_log()
    now = now_colombia()
    today = today_str()

    entry = {
        "channel": channel_name,
        "title": clip_title,
        "viral_score": viral_score,
        "published_at": now.isoformat(),
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
        "date": today,
        "hour_col": now.hour,
    }

    log["publications"].append(entry)

    # Mantener últimos 200 registros
    if len(log["publications"]) > 200:
        log["publications"] = log["publications"][-200:]

    # Contadores diarios
    if today not in log["daily_counts"]:
        log["daily_counts"][today] = {"total": 0, "by_channel": {}}

    log["daily_counts"][today]["total"] += 1
    ch_counts = log["daily_counts"][today].get("by_channel", {})
    ch_counts[channel_name] = ch_counts.get(channel_name, 0) + 1
    log["daily_counts"][today]["by_channel"] = ch_counts

    # Limpiar contadores de hace más de 7 días
    cutoff = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    log["daily_counts"] = {
        k: v for k, v in log["daily_counts"].items() if k >= cutoff
    }

    save_publish_log(log)
    return entry


def get_today_stats():
    """Obtiene estadísticas de publicaciones de hoy."""
    log = load_publish_log()
    today = today_str()
    config = load_config()
    limits = config.get("limits", {})
    max_day = limits.get("clips_per_day", 5)
    max_channel = limits.get("clips_per_channel", 2)

    daily = log.get("daily_counts", {}).get(today, {"total": 0, "by_channel": {}})

    return {
        "date": today,
        "total_published": daily["total"],
        "max_per_day": max_day,
        "remaining": max(0, max_day - daily["total"]),
        "by_channel": daily.get("by_channel", {}),
        "max_per_channel": max_channel,
    }


# ─── Verificaciones de Publicación ────────────────────────────────────────────

def can_publish_now(channel_name=None):
    """
    Verifica si se puede publicar ahora.
    Retorna dict con can_publish, reason, next_available.
    """
    config = load_config()
    limits = config.get("limits", {})
    max_day = limits.get("clips_per_day", 5)
    max_channel = limits.get("clips_per_channel", 2)
    min_hours = limits.get("min_hours_between_posts", 2)

    now = now_colombia()
    hour = now.hour
    log = load_publish_log()
    today = today_str()

    # Regla 1: No publicar entre 1am-6am Colombia
    if 1 <= hour < 6:
        next_avail = now.replace(hour=6, minute=0, second=0, microsecond=0)
        if next_avail <= now:
            next_avail += timedelta(days=1)
        return {
            "can_publish": False,
            "reason": f"Horario nocturno ({hour}:00 COL). No publicar 1am-6am.",
            "next_available": next_avail.isoformat(),
            "next_available_label": "6:00 AM COL",
        }

    # Regla 2: Máximo clips por día
    daily = log.get("daily_counts", {}).get(today, {"total": 0, "by_channel": {}})
    if daily["total"] >= max_day:
        tomorrow = (now + timedelta(days=1)).replace(hour=7, minute=0, second=0, microsecond=0)
        return {
            "can_publish": False,
            "reason": f"Límite diario alcanzado ({daily['total']}/{max_day})",
            "next_available": tomorrow.isoformat(),
            "next_available_label": "Mañana 7:00 AM COL",
        }

    # Regla 3: Máximo clips por canal por día
    if channel_name:
        ch_count = daily.get("by_channel", {}).get(channel_name, 0)
        if ch_count >= max_channel:
            tomorrow = (now + timedelta(days=1)).replace(hour=7, minute=0, second=0, microsecond=0)
            return {
                "can_publish": False,
                "reason": f"Canal {channel_name}: límite diario ({ch_count}/{max_channel})",
                "next_available": tomorrow.isoformat(),
                "next_available_label": f"Mañana ({channel_name})",
            }

    # Regla 4: Mínimo 2 horas entre publicaciones
    recent_pubs = [
        p for p in log.get("publications", [])
        if p.get("date") == today
    ]
    if recent_pubs:
        last_pub_time = recent_pubs[-1].get("published_at", "")
        try:
            last_dt = datetime.fromisoformat(last_pub_time)
            hours_since = (now - last_dt).total_seconds() / 3600
            if hours_since < min_hours:
                next_avail = last_dt + timedelta(hours=min_hours)
                mins_left = int((min_hours - hours_since) * 60)
                return {
                    "can_publish": False,
                    "reason": f"Muy pronto. Última pub hace {hours_since:.1f}h (mín {min_hours}h)",
                    "next_available": next_avail.isoformat(),
                    "next_available_label": f"En {mins_left} minutos",
                    "minutes_remaining": mins_left,
                }
        except Exception:
            pass

    # Todo OK
    return {
        "can_publish": True,
        "reason": "OK",
        "current_hour_col": hour,
        "is_peak": _is_peak_hour(hour, now.minute),
        "peak_priority": _get_peak_priority(hour, now.minute),
        "published_today": daily["total"],
        "remaining_today": max_day - daily["total"],
    }


def _is_peak_hour(hour, minute=0):
    """Verifica si la hora actual es horario peak."""
    for peak in PEAK_HOURS:
        ph = peak["hour"]
        # Ventana de ±30 minutos alrededor del peak
        if abs(hour - ph) == 0 or (hour == ph - 1 and minute >= 30) or (hour == ph + 1 and minute <= 30):
            return True
    return False


def _get_peak_priority(hour, minute=0):
    """Retorna la prioridad del peak actual."""
    best = "none"
    priorities = {"none": 0, "medium": 1, "high": 2, "highest": 3}

    for peak in PEAK_HOURS:
        ph = peak["hour"]
        if abs(hour - ph) <= 1:
            p = peak["priority"]
            if priorities.get(p, 0) > priorities.get(best, 0):
                best = p
    return best


def get_next_publish_slot():
    """Calcula el próximo slot de publicación disponible."""
    now = now_colombia()
    check = can_publish_now()

    if check["can_publish"] and check.get("is_peak"):
        return {
            "slot": now.isoformat(),
            "label": "¡AHORA! (horario peak)",
            "priority": check.get("peak_priority", "medium"),
            "wait_seconds": 0,
        }

    # Buscar próximo peak disponible
    for offset_days in range(2):
        check_date = now + timedelta(days=offset_days)
        for peak in PEAK_HOURS:
            slot = check_date.replace(
                hour=peak["hour"], minute=peak["minute"],
                second=0, microsecond=0
            )
            if slot > now:
                # Verificar que podemos publicar en ese momento
                wait = (slot - now).total_seconds()
                day_label = "Hoy" if offset_days == 0 else "Mañana"
                return {
                    "slot": slot.isoformat(),
                    "label": f"{day_label} {peak['hour']}:{peak['minute']:02d} COL ({peak['label']})",
                    "priority": peak["priority"],
                    "wait_seconds": int(wait),
                    "wait_minutes": int(wait / 60),
                }

    # Fallback
    return {
        "slot": (now + timedelta(hours=2)).isoformat(),
        "label": "En 2 horas",
        "priority": "low",
        "wait_seconds": 7200,
    }


# ─── Generador de Metadata ───────────────────────────────────────────────────

def generate_metadata(channel_name, stream_title="", viral_score=0):
    """
    Genera metadata optimizada para TikTok:
    - Descripción con CTA rotativo
    - Hashtags (base + canal + trending)
    - Título completo
    """
    config = load_config()
    limits = config.get("limits", {})
    referral_link = config.get("kick_referral", "")

    channel_lower = channel_name.lower()

    # ── Descripción (rotar templates) ──
    pub_log = load_publish_log()
    pub_count = len(pub_log.get("publications", []))

    # Cada 5 publicaciones, usar template con referral
    if referral_link and pub_count > 0 and pub_count % 5 == 0:
        description = REFERRAL_TEMPLATE.format(
            canal=channel_name,
            referral_link=referral_link,
        )
    else:
        template_idx = pub_count % len(DESCRIPTION_TEMPLATES)
        description = DESCRIPTION_TEMPLATES[template_idx].format(canal=channel_name)

    # Añadir título del stream si existe
    if stream_title:
        # Limpiar título
        clean_title = stream_title.strip()[:60]
        description = f"{clean_title} | {description}"

    # ── Hashtags ──
    tags = list(HASHTAGS_BASE)

    # Tags del canal
    ch_tags = HASHTAGS_POR_CANAL.get(channel_lower, [f"#{channel_lower}"])
    tags.extend(ch_tags)

    # Tags de score viral
    if viral_score >= 8:
        tags.extend(["#momentoepico", "#viral", "#top"])
    elif viral_score >= 6:
        tags.extend(["#clipazo", "#momentazo"])

    # Tags trending (estáticos pero efectivos para Colombia)
    trending_seasonal = _get_seasonal_tags()
    tags.extend(trending_seasonal)

    # Deduplicar manteniendo orden
    seen = set()
    unique_tags = []
    for tag in tags:
        t = tag.lower().strip()
        if t not in seen and t:
            seen.add(t)
            unique_tags.append(tag)

    # ── Título completo ──
    hashtags_str = " ".join(unique_tags)
    max_len = 150

    # Construir título final
    full_title = f"{description} {hashtags_str}"

    if len(full_title) > max_len:
        available = max_len - len(description) - 1
        trimmed = ""
        for tag in unique_tags:
            if len(trimmed) + len(tag) + 1 <= available:
                trimmed += f" {tag}"
            else:
                break
        full_title = f"{description}{trimmed}"

    return {
        "title": full_title[:max_len],
        "description": description,
        "hashtags": unique_tags,
        "hashtags_str": hashtags_str,
        "channel": channel_name,
        "viral_score": viral_score,
        "template_index": pub_count % len(DESCRIPTION_TEMPLATES),
        "includes_referral": referral_link and pub_count > 0 and pub_count % 5 == 0,
    }


def _get_seasonal_tags():
    """Tags trending estacionales/temporales."""
    now = now_colombia()
    month = now.month
    weekday = now.weekday()

    tags = []

    # Por día de la semana
    if weekday < 5:
        tags.append("#entretenimiento")
    else:
        tags.append("#findesemana")

    # Por mes/temporada
    if month == 12:
        tags.extend(["#navidad", "#diciembre"])
    elif month == 1:
        tags.append("#añonuevo")
    elif month in (6, 7):
        tags.append("#vacaciones")
    elif month == 10:
        tags.append("#halloween")
    elif month == 2:
        tags.append("#febrero")
    elif month == 3:
        tags.append("#marzo")

    # Tags siempre populares en LATAM
    tags.extend(["#latinoamerica", "#gaming"])

    return tags


# ─── Scraping TikTok Trending (básico) ───────────────────────────────────────

def get_trending_hashtags():
    """
    Intenta obtener hashtags trending de TikTok.
    Fallback a tags populares estáticos si falla.
    """
    try:
        from curl_cffi import requests as cffi_requests
        session = cffi_requests.Session(impersonate="chrome131")

        res = session.get(
            "https://www.tiktok.com/api/explore/item_list/",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://www.tiktok.com/explore",
            },
            params={"count": 20, "categoryType": 0},
            timeout=15,
        )

        if res.status_code == 200:
            data = res.json()
            items = data.get("itemList", data.get("items", []))
            tags = set()
            for item in items:
                for challenge in item.get("challenges", []):
                    title = challenge.get("title", "")
                    if title and len(title) < 30:
                        tags.add(f"#{title}")
            if tags:
                return list(tags)[:10]

    except Exception:
        pass

    # Fallback: tags populares fijos
    return [
        "#fyp", "#foryoupage", "#trending", "#viral",
        "#parati", "#humor", "#gaming", "#stream",
    ]


# ─── Cola Inteligente ─────────────────────────────────────────────────────────

def should_queue_or_publish(channel_name, viral_score=0):
    """
    Decide si publicar ahora o encolar para el próximo slot peak.
    Retorna acción recomendada.
    """
    check = can_publish_now(channel_name)

    if not check["can_publish"]:
        next_slot = get_next_publish_slot()
        return {
            "action": "queue",
            "reason": check["reason"],
            "next_slot": next_slot,
            "channel": channel_name,
            "viral_score": viral_score,
        }

    # Si es horario peak y score alto → publicar inmediatamente
    is_peak = check.get("is_peak", False)
    priority = check.get("peak_priority", "none")

    if is_peak and viral_score >= 7:
        return {
            "action": "publish_now",
            "reason": f"Peak hour + viral score {viral_score}",
            "priority": priority,
            "channel": channel_name,
        }

    if is_peak:
        return {
            "action": "publish_now",
            "reason": f"Peak hour ({priority})",
            "priority": priority,
            "channel": channel_name,
        }

    # No es peak pero se puede publicar
    if viral_score >= 8:
        return {
            "action": "publish_now",
            "reason": f"Score viral muy alto ({viral_score}) — publicar ya",
            "priority": "high",
            "channel": channel_name,
        }

    # Score medio, esperar al próximo peak
    next_slot = get_next_publish_slot()
    wait_min = next_slot.get("wait_minutes", 0)

    if wait_min <= 30:
        return {
            "action": "queue_short",
            "reason": f"Próximo peak en {wait_min} min — esperando",
            "next_slot": next_slot,
            "channel": channel_name,
        }

    # Si la espera es larga, publicar igual
    return {
        "action": "publish_now",
        "reason": f"Espera larga ({wait_min}min), publicando ahora",
        "priority": "low",
        "channel": channel_name,
    }


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    if len(args) == 0 or args[0] == "--stats":
        stats = get_today_stats()
        stats["current_time_col"] = now_colombia().strftime("%Y-%m-%d %H:%M:%S COL")
        stats["next_slot"] = get_next_publish_slot()
        print(json.dumps(stats, ensure_ascii=False))
        return

    if args[0] == "--can-publish":
        channel = args[1] if len(args) > 1 else None
        result = can_publish_now(channel)
        print(json.dumps(result, ensure_ascii=False))
        return

    if args[0] == "--next-slot":
        result = get_next_publish_slot()
        result["current_col"] = now_colombia().strftime("%H:%M COL")
        print(json.dumps(result, ensure_ascii=False))
        return

    if args[0] == "--metadata":
        channel = args[1] if len(args) > 1 else "unknown"
        title = args[2] if len(args) > 2 else ""
        score = float(args[3]) if len(args) > 3 else 0
        result = generate_metadata(channel, title, score)
        print(json.dumps(result, ensure_ascii=False))
        return

    if args[0] == "--record":
        channel = args[1] if len(args) > 1 else "unknown"
        title = args[2] if len(args) > 2 else ""
        score = float(args[3]) if len(args) > 3 else 0
        entry = record_publication(channel, title, score)
        print(json.dumps(entry, ensure_ascii=False))
        return

    if args[0] == "--should-publish":
        channel = args[1] if len(args) > 1 else "unknown"
        score = float(args[2]) if len(args) > 2 else 0
        result = should_queue_or_publish(channel, score)
        print(json.dumps(result, ensure_ascii=False))
        return

    if args[0] == "--trending":
        tags = get_trending_hashtags()
        print(json.dumps(tags, ensure_ascii=False))
        return

    # Default: stats
    stats = get_today_stats()
    print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()
