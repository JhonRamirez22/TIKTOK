#!/usr/bin/env python3
"""
KickClipBot — publish_optimizer.py (Senior Engineer Edition)
═══════════════════════════════════════════════════════════════
Motor dinámico de metadatos y estrategias de publicación optimizadas
para el algoritmo de TikTok.

Implementa los 6 Tiers de Hashtags y generación de descripciones virales.
"""

import json
import sys
import os
import random
from datetime import datetime, timedelta, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
COL_TZ = timezone(timedelta(hours=-5))

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

config = load_config()
hashtag_engine = config.get("hashtag_engine", {})
tiktok_config = config.get("tiktok_settings", {}).get("post_config", {})

def generate_title(channel, asr_quote=""):
    """
    Genera el título usando una frase extraída del audio ASR.
    Fallback dinámico si el ASR está vacío.
    """
    if asr_quote and len(asr_quote) > 10:
        clean_quote = asr_quote.strip().replace('\n', ' ')
        if len(clean_quote) > 60:
            clean_quote = clean_quote[:57] + "..."
        emojis = ["😱🔥", "😂💀", "🤯🚀", "👀🍿"]
        return f"{random.choice(['🎬', '🔥', '💥', '🗣️'])} \"{clean_quote}\" {random.choice(emojis)}"
    else:
        fallbacks = [
            f"{channel} acaba de hacer ESTO en directo 👀",
            f"El stream de {channel} se salió de control 💀",
            f"Nadie esperaba esto de {channel} 😱🔥",
            f"Momento ÉPICO de {channel} en Kick 🎮🚀"
        ]
        return random.choice(fallbacks)

def get_game_category(channel):
    """Consulta la API de Kick para sacar la categoría/juego en vivo (Tier 4)."""
    try:
        from curl_cffi import requests
        res = requests.get(f"https://kick.com/api/v1/channels/{channel}", impersonate="chrome120", timeout=5)
        if res.status_code == 200:
            data = res.json()
            cat = data.get("livestream", {}).get("categories", [{}])[0].get("name", "")
            if cat:
                # Normalizar a hashtag: "Grand Theft Auto V" -> "#grandtheftautov"
                return f"#{cat.lower().replace(' ', '').replace(':', '').replace('-', '')}"
    except Exception:
        pass
    return "#gaming"

def build_hashtags(channel):
    """Mezcla los 6 Tiers de hashtags - MAXIMO 20"""
    tags = []
    
    # Tier 1: Masivos (2-3)
    t1 = hashtag_engine.get("tier1_massive", ["#fyp", "#parati", "#viral"])
    tags.extend(random.sample(t1, 2))
    
    # Tier 2: Gaming general (3-4)
    t2 = hashtag_engine.get("tier2_gaming", ["#gaming", "#twitch", "#streamer", "#clips"])
    tags.extend(random.sample(t2, 3))
    
    # Tier 3: Plataforma (2-3)
    t3 = hashtag_engine.get("tier3_platform", ["#kick", "#kickstreaming", "#kickclips"])
    tags.extend(random.sample(t2, 3))
    
    # Tier 4: Juego Específico
    game_tag = get_game_category(channel)
    tags.append(game_tag)
    
    # Tier 5: Momento (2-3)
    t5 = hashtag_engine.get("tier5_moment", ["#clip", "#highlights", "#bestmoment"])
    tags.extend(random.sample(t5, 2))
    
    # Tier 6: Región/Idioma (1-2)
    t6 = hashtag_engine.get("tier6_region_es", ["#españa", "#latinos", "#colombia"])
    tags.extend(random.sample(t6, 1))
    
    # Custom del canal
    tags.append(f"#{channel.lower()}")
    
    # LIMITE MAXIMO 20 hashtags
    tags = tags[:20]
    
    # Deduplicar manteniendo orden
    seen = set()
    unique_tags = []
    for tag in tags:
        t = tag.lower().strip()
        if t not in seen and t:
            seen.add(t)
            unique_tags.append(tag)
    
    # UNIR CON ESPACIOS para que TikTok los detecte como links
    return unique_tags

def generate_payload(channel, asr_quote=""):
    """
    Construye el payload completo de publicación (Título, Descripción, Tags, Configuración)
    """
    title = generate_title(channel, asr_quote)
    hashtags = build_hashtags(channel)
    
    # La descripción incluye primero el título llamativo que aparece en preview
    # Y luego los hashtags. (SIN LINKS)
    description = f"{title}\n\n{' '.join(hashtags)}"
    
    payload = {
        "channel": channel,
        "title": title[:150], # Limitar a 150 chars max
        "description": description[:2200], # Tiktok limits description to 2200
        "hashtags": hashtags,
        "post_config": {
            "duet": tiktok_config.get("duet", True),
            "stitch": tiktok_config.get("stitch", True),
            "privacy": tiktok_config.get("privacy", "public"),
            "allow_comments": tiktok_config.get("allow_comments", True)
        }
    }
    
    return payload

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--generate":
        channel = sys.argv[2] if len(sys.argv) > 2 else "westcol"
        quote = sys.argv[3] if len(sys.argv) > 3 else "No me lo creo mari*a, mira esto!"
        print(json.dumps(generate_payload(channel, quote), ensure_ascii=False, indent=2))
    else:
        print("Uso: python3 publish_optimizer.py --generate <canal> [asr_quote]")