#!/usr/bin/env python3
"""
KickClipBot — monetization.py
═══════════════════════════════════════════════════════════════
Sistema de tracking de métricas y monetización para TikTok.

Funcionalidades:
  - Registro de views, likes, comentarios por clip en metrics.json
  - CTR promedio por canal
  - Identificación de tipo de momento con más engagement
  - Reporte semanal automático
  - Milestones de seguidores (500, 1000, 10000)
  - Instrucciones de monetización según milestone
  - Gestión de link de referido de Kick

Uso:
    python3 monetization.py --record-clip <canal> <views> <likes> <comments>
    python3 monetization.py --update-followers <count>
    python3 monetization.py --report                → reporte semanal
    python3 monetization.py --best-clips             → top clips por engagement
    python3 monetization.py --ctr-by-channel          → CTR por canal
    python3 monetization.py --check-milestones        → verifica milestones
    python3 monetization.py --dashboard               → resumen para dashboard

Salida: JSON a stdout
"""

import json
import sys
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ─── Configuración ────────────────────────────────────────────────────────────

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH  = os.path.join(SCRIPT_DIR, "config.json")
METRICS_PATH = os.path.join(SCRIPT_DIR, "metrics.json")

COL_TZ = timezone(timedelta(hours=-5))

MILESTONES = [
    {
        "followers": 500,
        "label": "500 Seguidores",
        "emoji": "🎉",
        "message": "¡Felicidades! Llegaste a 500 seguidores.",
        "instructions": [
            "Tu cuenta está creciendo. Mantén la consistencia.",
            "Publica 3-5 clips al día en horarios peak.",
            "Interactúa con comentarios para aumentar engagement.",
        ],
    },
    {
        "followers": 1000,
        "label": "1,000 Seguidores — Creator Fund",
        "emoji": "💰",
        "message": "¡1000 seguidores! Ya puedes monetizar.",
        "instructions": [
            "ACTIVAR TIKTOK CREATOR FUND:",
            "1. Ve a Perfil → Menú (☰) → Creator Tools",
            "2. Toca 'TikTok Creator Fund' o 'Creativity Program'",
            "3. Verifica que cumples: 1000+ followers, 10000+ views en 30 días",
            "4. Acepta los términos y activa la monetización",
            "5. Conecta tu método de pago (PayPal / cuenta bancaria)",
            "",
            "TIPS PARA MAXIMIZAR INGRESOS:",
            "- Videos más largos (>1 min) generan más RPM",
            "- El programa de creatividad paga más que el Creator Fund básico",
            "- Mantén consistencia: 3-5 videos diarios",
        ],
    },
    {
        "followers": 5000,
        "label": "5,000 Seguidores — LIVE",
        "emoji": "🔴",
        "message": "¡5000 seguidores! Puedes hacer LIVE en TikTok.",
        "instructions": [
            "ACTIVAR TIKTOK LIVE:",
            "1. Ya puedes hacer transmisiones en vivo",
            "2. Los viewers pueden enviarte regalos virtuales (monedas)",
            "3. Programa lives cuando tus streamers favoritos estén en vivo",
            "4. Reacciona a clips en vivo para más engagement",
        ],
    },
    {
        "followers": 10000,
        "label": "10,000 Seguidores — TikTok Series & Shop",
        "emoji": "🏆",
        "message": "¡10K! Desbloqueas funciones premium de monetización.",
        "instructions": [
            "ACTIVAR TIKTOK SERIES:",
            "1. Ve a Creator Tools → Series",
            "2. Crea contenido premium (compilaciones exclusivas)",
            "3. Cobra por acceso a series de clips exclusivos",
            "",
            "ACTIVAR TIKTOK SHOP:",
            "1. Ve a Creator Tools → TikTok Shop",
            "2. Puedes vender merch o productos de afiliados",
            "3. Agrega links de productos en tus videos",
            "",
            "BRAND DEALS:",
            "1. Con 10K+ puedes contactar marcas de gaming",
            "2. Usa TikTok Creator Marketplace para deals",
            "3. Cobra $50-$200 por video patrocinado",
        ],
    },
    {
        "followers": 50000,
        "label": "50,000 Seguidores",
        "emoji": "⭐",
        "message": "¡50K! Eres un creador establecido.",
        "instructions": [
            "A este nivel puedes:",
            "- Negociar deals directos con streamers de Kick",
            "- Crear tu propia marca de clips",
            "- Cobrar $200-$1000 por videos patrocinados",
            "- Considerar expandir a YouTube Shorts e Instagram Reels",
        ],
    },
    {
        "followers": 100000,
        "label": "100,000 Seguidores",
        "emoji": "👑",
        "message": "¡100K! Eres una referencia en clips de Kick en TikTok.",
        "instructions": [
            "Estrategias nivel 100K:",
            "- Multi-plataforma: TikTok + YouTube + Instagram",
            "- Manager de contenido o equipo",
            "- Partnerships directos con Kick.com",
            "- Ingresos estimados: $2000-$10000/mes",
        ],
    },
]


def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def load_metrics():
    try:
        with open(METRICS_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {
            "clips": [],
            "followers": {"current": 0, "history": [], "milestones_reached": []},
            "totals": {"views": 0, "likes": 0, "comments": 0, "shares": 0},
        }


def save_metrics(metrics):
    try:
        with open(METRICS_PATH, "w") as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[monetization] Error guardando metrics: {e}", file=sys.stderr)


def now_col():
    return datetime.now(COL_TZ)


# ─── Registro de Clips ───────────────────────────────────────────────────────

def record_clip_metrics(channel, views=0, likes=0, comments=0, shares=0,
                        title="", viral_score=0, clip_type=""):
    """Registra métricas de un clip publicado."""
    metrics = load_metrics()
    now = now_col()

    entry = {
        "channel": channel,
        "views": views,
        "likes": likes,
        "comments": comments,
        "shares": shares,
        "engagement_rate": round((likes + comments + shares) / max(views, 1) * 100, 2),
        "title": title[:100],
        "viral_score": viral_score,
        "clip_type": clip_type,
        "recorded_at": now.isoformat(),
        "date": now.strftime("%Y-%m-%d"),
    }

    metrics["clips"].append(entry)

    # Mantener últimos 1000 clips
    if len(metrics["clips"]) > 1000:
        metrics["clips"] = metrics["clips"][-1000:]

    # Actualizar totales
    metrics["totals"]["views"] += views
    metrics["totals"]["likes"] += likes
    metrics["totals"]["comments"] += comments
    metrics["totals"]["shares"] += shares

    save_metrics(metrics)
    return entry


def update_clip_metrics(clip_index, views=None, likes=None, comments=None, shares=None):
    """Actualiza métricas de un clip existente (refresh de stats)."""
    metrics = load_metrics()

    if clip_index < 0 or clip_index >= len(metrics["clips"]):
        return {"error": "Índice de clip inválido"}

    clip = metrics["clips"][clip_index]
    old_views = clip.get("views", 0)
    old_likes = clip.get("likes", 0)
    old_comments = clip.get("comments", 0)
    old_shares = clip.get("shares", 0)

    if views is not None:
        clip["views"] = views
        metrics["totals"]["views"] += (views - old_views)
    if likes is not None:
        clip["likes"] = likes
        metrics["totals"]["likes"] += (likes - old_likes)
    if comments is not None:
        clip["comments"] = comments
        metrics["totals"]["comments"] += (comments - old_comments)
    if shares is not None:
        clip["shares"] = shares
        metrics["totals"]["shares"] += (shares - old_shares)

    clip["engagement_rate"] = round(
        (clip.get("likes", 0) + clip.get("comments", 0) + clip.get("shares", 0))
        / max(clip.get("views", 1), 1) * 100, 2
    )
    clip["updated_at"] = now_col().isoformat()

    save_metrics(metrics)
    return clip


# ─── Seguidores y Milestones ──────────────────────────────────────────────────

def update_followers(count):
    """Actualiza el conteo de seguidores y verifica milestones."""
    metrics = load_metrics()
    old_count = metrics["followers"]["current"]
    metrics["followers"]["current"] = count

    # Registrar en historial
    metrics["followers"]["history"].append({
        "count": count,
        "date": now_col().isoformat(),
    })

    # Mantener últimos 365 registros
    if len(metrics["followers"]["history"]) > 365:
        metrics["followers"]["history"] = metrics["followers"]["history"][-365:]

    # Verificar milestones
    new_milestones = []
    reached = set(metrics["followers"].get("milestones_reached", []))

    for ms in MILESTONES:
        ms_key = str(ms["followers"])
        if count >= ms["followers"] and ms_key not in reached:
            reached.add(ms_key)
            new_milestones.append(ms)

    metrics["followers"]["milestones_reached"] = list(reached)
    save_metrics(metrics)

    result = {
        "followers": count,
        "previous": old_count,
        "change": count - old_count,
        "new_milestones": new_milestones,
    }

    return result


def check_milestones():
    """Verifica qué milestones se han alcanzado y cuáles faltan."""
    metrics = load_metrics()
    followers = metrics["followers"]["current"]
    reached = set(metrics["followers"].get("milestones_reached", []))

    result = {
        "followers": followers,
        "milestones": [],
    }

    for ms in MILESTONES:
        ms_key = str(ms["followers"])
        is_reached = ms_key in reached or followers >= ms["followers"]
        progress = min(round(followers / ms["followers"] * 100, 1), 100)

        result["milestones"].append({
            "target": ms["followers"],
            "label": ms["label"],
            "emoji": ms["emoji"],
            "reached": is_reached,
            "progress_pct": progress,
            "remaining": max(0, ms["followers"] - followers),
            "message": ms["message"] if is_reached else "",
            "instructions": ms["instructions"] if is_reached else [],
        })

    # Próximo milestone
    for ms in MILESTONES:
        if followers < ms["followers"]:
            result["next_milestone"] = {
                "target": ms["followers"],
                "remaining": ms["followers"] - followers,
                "label": ms["label"],
            }
            break

    return result


# ─── Análisis y Reportes ─────────────────────────────────────────────────────

def get_ctr_by_channel():
    """Calcula CTR (engagement rate) promedio por canal."""
    metrics = load_metrics()
    channel_stats = defaultdict(lambda: {
        "views": 0, "likes": 0, "comments": 0, "shares": 0, "clips": 0,
    })

    for clip in metrics["clips"]:
        ch = clip.get("channel", "unknown")
        channel_stats[ch]["views"] += clip.get("views", 0)
        channel_stats[ch]["likes"] += clip.get("likes", 0)
        channel_stats[ch]["comments"] += clip.get("comments", 0)
        channel_stats[ch]["shares"] += clip.get("shares", 0)
        channel_stats[ch]["clips"] += 1

    result = []
    for ch, stats in sorted(channel_stats.items(), key=lambda x: x[1]["views"], reverse=True):
        total_engagement = stats["likes"] + stats["comments"] + stats["shares"]
        ctr = round(total_engagement / max(stats["views"], 1) * 100, 2)
        result.append({
            "channel": ch,
            "clips": stats["clips"],
            "views": stats["views"],
            "likes": stats["likes"],
            "comments": stats["comments"],
            "shares": stats["shares"],
            "avg_views": round(stats["views"] / max(stats["clips"], 1)),
            "avg_engagement_rate": ctr,
        })

    return result


def get_best_clips(limit=10):
    """Retorna los clips con más engagement."""
    metrics = load_metrics()
    clips = sorted(
        metrics["clips"],
        key=lambda c: c.get("views", 0),
        reverse=True,
    )
    return clips[:limit]


def get_engagement_by_type():
    """Analiza engagement por tipo de momento/viral_score."""
    metrics = load_metrics()
    score_buckets = {
        "bajo (1-4)": {"views": 0, "engagement": 0, "clips": 0},
        "medio (5-6)": {"views": 0, "engagement": 0, "clips": 0},
        "alto (7-8)": {"views": 0, "engagement": 0, "clips": 0},
        "viral (9-10)": {"views": 0, "engagement": 0, "clips": 0},
    }

    for clip in metrics["clips"]:
        score = clip.get("viral_score", 0)
        views = clip.get("views", 0)
        eng = clip.get("likes", 0) + clip.get("comments", 0) + clip.get("shares", 0)

        if score >= 9:
            bucket = "viral (9-10)"
        elif score >= 7:
            bucket = "alto (7-8)"
        elif score >= 5:
            bucket = "medio (5-6)"
        else:
            bucket = "bajo (1-4)"

        score_buckets[bucket]["views"] += views
        score_buckets[bucket]["engagement"] += eng
        score_buckets[bucket]["clips"] += 1

    result = []
    for label, data in score_buckets.items():
        avg_views = round(data["views"] / max(data["clips"], 1))
        avg_eng = round(data["engagement"] / max(data["clips"], 1))
        ctr = round(data["engagement"] / max(data["views"], 1) * 100, 2) if data["views"] > 0 else 0
        result.append({
            "score_range": label,
            "clips": data["clips"],
            "total_views": data["views"],
            "avg_views": avg_views,
            "avg_engagement": avg_eng,
            "ctr": ctr,
        })

    return result


def generate_weekly_report():
    """Genera reporte semanal de métricas."""
    metrics = load_metrics()
    now = now_col()
    week_ago = now - timedelta(days=7)
    week_ago_str = week_ago.strftime("%Y-%m-%d")

    # Filtrar clips de la última semana
    weekly_clips = [
        c for c in metrics["clips"]
        if c.get("date", "") >= week_ago_str
    ]

    total_views = sum(c.get("views", 0) for c in weekly_clips)
    total_likes = sum(c.get("likes", 0) for c in weekly_clips)
    total_comments = sum(c.get("comments", 0) for c in weekly_clips)
    total_shares = sum(c.get("shares", 0) for c in weekly_clips)
    total_clips = len(weekly_clips)

    # Mejor clip de la semana
    best_clip = max(weekly_clips, key=lambda c: c.get("views", 0)) if weekly_clips else None

    # Mejor canal de la semana
    channel_views = defaultdict(int)
    for c in weekly_clips:
        channel_views[c.get("channel", "unknown")] += c.get("views", 0)
    best_channel = max(channel_views.items(), key=lambda x: x[1])[0] if channel_views else "N/A"

    # Followers
    followers = metrics["followers"]["current"]
    followers_hist = metrics["followers"].get("history", [])
    followers_week_ago = 0
    for fh in followers_hist:
        if fh.get("date", "")[:10] >= week_ago_str:
            followers_week_ago = fh.get("count", 0)
            break
    follower_growth = followers - followers_week_ago

    # Engagement rate promedio
    avg_eng = round(
        (total_likes + total_comments + total_shares)
        / max(total_views, 1) * 100, 2
    )

    report = {
        "period": f"{week_ago_str} — {now.strftime('%Y-%m-%d')}",
        "generated_at": now.isoformat(),
        "summary": {
            "total_clips": total_clips,
            "total_views": total_views,
            "total_likes": total_likes,
            "total_comments": total_comments,
            "total_shares": total_shares,
            "avg_engagement_rate": avg_eng,
            "avg_views_per_clip": round(total_views / max(total_clips, 1)),
        },
        "followers": {
            "current": followers,
            "growth_this_week": follower_growth,
            "growth_pct": round(follower_growth / max(followers_week_ago, 1) * 100, 1),
        },
        "best_clip": {
            "channel": best_clip.get("channel", "N/A") if best_clip else "N/A",
            "title": best_clip.get("title", "") if best_clip else "",
            "views": best_clip.get("views", 0) if best_clip else 0,
            "engagement_rate": best_clip.get("engagement_rate", 0) if best_clip else 0,
        },
        "best_channel": best_channel,
        "engagement_by_score": get_engagement_by_type(),
        "ctr_by_channel": get_ctr_by_channel(),
    }

    # Guardar reporte
    reports_dir = os.path.join(SCRIPT_DIR, "logs")
    os.makedirs(reports_dir, exist_ok=True)
    report_file = os.path.join(reports_dir, f"weekly_report_{now.strftime('%Y%m%d')}.json")
    try:
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

    return report


def get_dashboard_summary():
    """Resumen compacto para el dashboard de Node.js."""
    metrics = load_metrics()
    followers = metrics["followers"]["current"]
    totals = metrics["totals"]
    clips = metrics["clips"]

    # Mejor clip
    best = max(clips, key=lambda c: c.get("views", 0)) if clips else None

    # Clips de hoy
    today = now_col().strftime("%Y-%m-%d")
    today_clips = [c for c in clips if c.get("date") == today]
    today_views = sum(c.get("views", 0) for c in today_clips)

    # Próximo milestone
    next_ms = None
    for ms in MILESTONES:
        if followers < ms["followers"]:
            next_ms = {
                "target": ms["followers"],
                "remaining": ms["followers"] - followers,
                "label": ms["label"],
                "emoji": ms["emoji"],
            }
            break

    return {
        "followers": followers,
        "total_views": totals.get("views", 0),
        "total_likes": totals.get("likes", 0),
        "total_clips_tracked": len(clips),
        "today_clips": len(today_clips),
        "today_views": today_views,
        "best_clip": {
            "channel": best.get("channel", "N/A") if best else "N/A",
            "title": (best.get("title", "Sin título") if best else "N/A")[:40],
            "views": best.get("views", 0) if best else 0,
        },
        "next_milestone": next_ms,
        "avg_engagement_rate": round(
            (totals.get("likes", 0) + totals.get("comments", 0))
            / max(totals.get("views", 1), 1) * 100, 2
        ),
    }


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    if len(args) == 0 or args[0] == "--dashboard":
        result = get_dashboard_summary()
        print(json.dumps(result, ensure_ascii=False))
        return

    if args[0] == "--record-clip":
        channel = args[1] if len(args) > 1 else "unknown"
        views = int(args[2]) if len(args) > 2 else 0
        likes = int(args[3]) if len(args) > 3 else 0
        comments = int(args[4]) if len(args) > 4 else 0
        shares = int(args[5]) if len(args) > 5 else 0
        title = args[6] if len(args) > 6 else ""
        score = float(args[7]) if len(args) > 7 else 0
        entry = record_clip_metrics(channel, views, likes, comments, shares, title, score)
        print(json.dumps(entry, ensure_ascii=False))
        return

    if args[0] == "--update-followers":
        count = int(args[1]) if len(args) > 1 else 0
        result = update_followers(count)
        print(json.dumps(result, ensure_ascii=False))
        return

    if args[0] == "--check-milestones":
        result = check_milestones()
        print(json.dumps(result, ensure_ascii=False))
        return

    if args[0] == "--report":
        report = generate_weekly_report()
        print(json.dumps(report, ensure_ascii=False))
        return

    if args[0] == "--best-clips":
        limit = int(args[1]) if len(args) > 1 else 10
        clips = get_best_clips(limit)
        print(json.dumps(clips, ensure_ascii=False))
        return

    if args[0] == "--ctr-by-channel":
        result = get_ctr_by_channel()
        print(json.dumps(result, ensure_ascii=False))
        return

    if args[0] == "--engagement-by-type":
        result = get_engagement_by_type()
        print(json.dumps(result, ensure_ascii=False))
        return

    # Default
    result = get_dashboard_summary()
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
