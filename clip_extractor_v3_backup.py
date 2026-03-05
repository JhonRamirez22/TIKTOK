#!/usr/bin/env python3
"""
KickClipBot — clip_extractor.py
═══════════════════════════════════════════════════════════════
Extrae clips de canales de Kick.com

Modos de operación:
  1. API: Obtiene clips existentes via API de Kick
  2. Streamlink+FFmpeg: Captura HLS en vivo directamente

Uso:
    python3 clip_extractor.py <canal> --api            → clips via API
    python3 clip_extractor.py <canal> --capture <dur>  → captura HLS en vivo
    python3 clip_extractor.py <canal> --best            → mejor clip reciente

Salida: JSON a stdout con path al archivo descargado
"""

import json
import sys
import os
import time
import random
import subprocess
import shutil
from datetime import datetime

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print(json.dumps({"error": "curl_cffi no instalado. Ejecuta: pip install curl_cffi"}))
    sys.exit(1)

# ─── Configuración ────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")

API_BASE = "https://kick.com/api/v1"
API_V2 = "https://kick.com/api/v2"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]


def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def ensure_dirs():
    """Crea directorios necesarios."""
    config = load_config()
    paths = config.get("paths", {})
    for key in ["clips_dir", "temp_dir"]:
        d = os.path.join(SCRIPT_DIR, paths.get(key, f"./{key.replace('_dir', '')}"))
        os.makedirs(d, exist_ok=True)
    return {
        "clips": os.path.join(SCRIPT_DIR, paths.get("clips_dir", "./clips")),
        "temp": os.path.join(SCRIPT_DIR, paths.get("temp_dir", "./temp")),
    }


def create_session():
    return cffi_requests.Session(impersonate="chrome131")


# ─── API: Obtener clips existentes ────────────────────────────────────────────

def get_clips_from_api(channel_name, sort_by="view_count", limit=10):
    """
    Obtiene clips existentes de un canal via API de Kick.
    sort_by: 'view_count', 'created_at', 'duration'
    """
    session = create_session()
    ua = random.choice(USER_AGENTS)
    clips_found = []

    # Primero obtener channel info para el slug/id
    try:
        info_res = session.get(
            f"{API_BASE}/channels/{channel_name}",
            headers={
                "User-Agent": ua,
                "Accept": "application/json",
                "Referer": f"https://kick.com/{channel_name}",
                "Origin": "https://kick.com",
            },
            timeout=20,
        )
        if info_res.status_code != 200:
            return {"error": f"No se pudo obtener info del canal: HTTP {info_res.status_code}", "clips": []}

        channel_data = info_res.json()
        channel_id = channel_data.get("id")
    except Exception as e:
        return {"error": f"Error obteniendo canal: {str(e)[:200]}", "clips": []}

    time.sleep(random.uniform(0.5, 1.0))

    # Obtener clips del canal (usar slug, no ID numérico)
    try:
        clips_url = f"{API_V2}/channels/{channel_name}/clips"
        # Kick API usa 'view' no 'view_count', 'date' no 'created_at'
        sort_map = {"view_count": "view", "created_at": "date", "duration": "duration"}
        api_sort = sort_map.get(sort_by, "view")
        params = {"cursor": "0", "sort": api_sort, "time": "all"}

        res = session.get(
            clips_url,
            params=params,
            headers={
                "User-Agent": ua,
                "Accept": "application/json",
                "Referer": f"https://kick.com/{channel_name}/clips",
                "Origin": "https://kick.com",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            },
            timeout=20,
        )

        if res.status_code == 200:
            data = res.json()
            raw_clips = []
            if isinstance(data, list):
                raw_clips = data
            elif isinstance(data, dict):
                raw_clips = data.get("clips", data.get("data", []))

            for clip in raw_clips[:limit]:
                clip_info = {
                    "id": clip.get("id"),
                    "title": clip.get("title", "Sin título"),
                    "duration": int(clip.get("duration", 0)),
                    "view_count": int(clip.get("views", clip.get("view_count", 0))),
                    "likes": int(clip.get("likes", clip.get("likes_count", 0))),
                    "video_url": clip.get("clip_url") or clip.get("video_url") or clip.get("url", ""),
                    "thumbnail_url": clip.get("thumbnail_url", ""),
                    "created_at": clip.get("created_at", ""),
                    "creator": clip.get("creator", {}).get("username", "unknown") if isinstance(clip.get("creator"), dict) else "unknown",
                    "channel": channel_name,
                    "category": clip.get("category", {}).get("name", "") if isinstance(clip.get("category"), dict) else "",
                }
                clips_found.append(clip_info)

        else:
            # Intentar endpoint alternativo
            alt_url = f"{API_V2}/channels/{channel_name}/clips"
            res2 = session.get(
                alt_url,
                params={"cursor": "0", "sort": "view", "time": "all"},
                headers={
                    "User-Agent": ua,
                    "Accept": "application/json",
                    "Referer": f"https://kick.com/{channel_name}/clips",
                    "Origin": "https://kick.com",
                },
                timeout=20,
            )
            if res2.status_code == 200:
                data2 = res2.json()
                raw_clips = data2 if isinstance(data2, list) else data2.get("clips", data2.get("data", []))
                for clip in raw_clips[:limit]:
                    clip_info = {
                        "id": clip.get("id"),
                        "title": clip.get("title", "Sin título"),
                        "duration": int(clip.get("duration", 0)),
                        "view_count": int(clip.get("views", clip.get("view_count", 0))),
                        "likes": int(clip.get("likes", clip.get("likes_count", 0))),
                        "video_url": clip.get("clip_url") or clip.get("video_url") or clip.get("url", ""),
                        "thumbnail_url": clip.get("thumbnail_url", ""),
                        "created_at": clip.get("created_at", ""),
                        "channel": channel_name,
                    }
                    clips_found.append(clip_info)

    except Exception as e:
        return {"error": f"Error obteniendo clips: {str(e)[:200]}", "clips": clips_found}

    # Ordenar
    if sort_by == "view_count":
        clips_found.sort(key=lambda c: c.get("view_count", 0), reverse=True)
    elif sort_by == "created_at":
        clips_found.sort(key=lambda c: c.get("created_at", ""), reverse=True)
    elif sort_by == "duration":
        clips_found.sort(key=lambda c: c.get("duration", 0), reverse=True)

    return {"channel": channel_name, "count": len(clips_found), "clips": clips_found, "error": None}


def download_clip(clip_info, output_dir=None):
    """
    Descarga un clip específico a disco.
    Soporta URLs directas (MP4) y HLS (.m3u8) via ffmpeg.
    Retorna path del archivo descargado.
    """
    dirs = ensure_dirs()
    if output_dir is None:
        output_dir = dirs["clips"]

    video_url = clip_info.get("video_url", "")
    if not video_url:
        return {"error": "No hay URL de video en el clip", "path": None}

    channel = clip_info.get("channel", "unknown")
    clip_id = clip_info.get("id", int(time.time()))
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{channel}_{clip_id}_{timestamp}.mp4"
    output_path = os.path.join(output_dir, filename)

    # Si es HLS (.m3u8), usar ffmpeg para descargar
    if ".m3u8" in video_url:
        return _download_hls_clip(video_url, output_path, clip_info)

    # Descarga directa (MP4)
    session = create_session()
    ua = random.choice(USER_AGENTS)

    try:
        res = session.get(
            video_url,
            headers={
                "User-Agent": ua,
                "Accept": "*/*",
                "Referer": f"https://kick.com/{channel}",
                "Origin": "https://kick.com",
            },
            timeout=120,
        )

        if res.status_code == 200:
            with open(output_path, "wb") as f:
                f.write(res.content)

            file_size = os.path.getsize(output_path)
            return {
                "path": output_path,
                "filename": filename,
                "size_bytes": file_size,
                "size_mb": round(file_size / (1024 * 1024), 2),
                "channel": channel,
                "clip_id": clip_id,
                "title": clip_info.get("title", ""),
                "error": None,
            }
        else:
            return {"error": f"HTTP {res.status_code} al descargar clip", "path": None}

    except Exception as e:
        return {"error": f"Error descargando: {str(e)[:200]}", "path": None}


# ─── HLS clip download helper ─────────────────────────────────────────────────

def _download_hls_clip(hls_url, output_path, clip_info):
    """Descarga un clip HLS (.m3u8) usando ffmpeg."""
    channel = clip_info.get("channel", "unknown")

    if not shutil.which("ffmpeg"):
        return {"error": "ffmpeg no instalado", "path": None}

    print(f"[clip_extractor] Descargando HLS clip: {os.path.basename(output_path)}...", file=sys.stderr)

    try:
        cmd = [
            "ffmpeg", "-y",
            "-i", hls_url,
            "-c", "copy",
            "-bsf:a", "aac_adtstoasc",
            "-movflags", "+faststart",
            output_path,
        ]

        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

        if proc.returncode != 0:
            # Retry con re-encoding
            cmd2 = [
                "ffmpeg", "-y",
                "-i", hls_url,
                "-c:v", "libx264", "-preset", "fast",
                "-c:a", "aac",
                "-movflags", "+faststart",
                output_path,
            ]
            proc2 = subprocess.run(cmd2, capture_output=True, text=True, timeout=180)
            if proc2.returncode != 0:
                return {"error": f"ffmpeg HLS falló: {proc2.stderr[-200:]}", "path": None}

        if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
            file_size = os.path.getsize(output_path)
            return {
                "path": output_path,
                "filename": os.path.basename(output_path),
                "size_bytes": file_size,
                "size_mb": round(file_size / (1024 * 1024), 2),
                "channel": channel,
                "clip_id": clip_info.get("id"),
                "title": clip_info.get("title", ""),
                "method": "hls_download",
                "error": None,
            }
        return {"error": "ffmpeg no generó archivo válido", "path": None}

    except subprocess.TimeoutExpired:
        return {"error": "Timeout descargando clip HLS", "path": None}
    except Exception as e:
        return {"error": f"Error HLS download: {str(e)[:200]}", "path": None}


# ─── Fallback: Captura HLS via Streamlink + FFmpeg ────────────────────────────

def capture_hls_stream(channel_name, duration_sec=30):
    """
    Captura un segmento del stream en vivo usando streamlink + ffmpeg.
    Fallback cuando no hay clips via API.
    """
    dirs = ensure_dirs()
    config = load_config()
    clip_cfg = config.get("clip_settings", {})
    max_dur = clip_cfg.get("max_duration_sec", 60)
    duration_sec = min(duration_sec, max_dur)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{channel_name}_capture_{timestamp}.mp4"
    output_path = os.path.join(dirs["clips"], filename)
    temp_path = os.path.join(dirs["temp"], f"_hls_{channel_name}_{timestamp}.ts")

    # Verificar que streamlink está disponible
    if not shutil.which("streamlink"):
        return {"error": "streamlink no instalado. Ejecuta: pip install streamlink", "path": None}

    # Verificar que ffmpeg está disponible
    if not shutil.which("ffmpeg"):
        return {"error": "ffmpeg no instalado. Ejecuta: brew install ffmpeg (macOS) o apt install ffmpeg (Linux)", "path": None}

    stream_url = f"https://kick.com/{channel_name}"

    try:
        # Paso 1: Obtener URL del stream HLS con streamlink
        print(f"[clip_extractor] Capturando {duration_sec}s de {channel_name}...", file=sys.stderr)

        # Usar streamlink para obtener la URL HLS
        sl_proc = subprocess.run(
            ["streamlink", "--stream-url", stream_url, "best"],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if sl_proc.returncode != 0:
            # Intentar con calidad alternativa
            sl_proc = subprocess.run(
                ["streamlink", "--stream-url", stream_url, "720p60,720p,480p,best"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if sl_proc.returncode != 0:
                return {
                    "error": f"streamlink no pudo obtener stream: {sl_proc.stderr[:200]}",
                    "path": None,
                }

        hls_url = sl_proc.stdout.strip()
        if not hls_url:
            return {"error": "streamlink no retornó URL de HLS", "path": None}

        # Paso 2: Capturar segmento con ffmpeg
        ffmpeg_cmd = [
            "ffmpeg",
            "-y",                          # Sobreescribir
            "-i", hls_url,                 # Input HLS
            "-t", str(duration_sec),       # Duración
            "-c", "copy",                  # Sin re-encoding (rápido)
            "-bsf:a", "aac_adtstoasc",    # Fix para HLS → MP4
            "-movflags", "+faststart",     # Streaming friendly
            output_path,
        ]

        ff_proc = subprocess.run(
            ffmpeg_cmd,
            capture_output=True,
            text=True,
            timeout=duration_sec + 60,
        )

        if ff_proc.returncode != 0:
            # Intentar con re-encoding si copy falla
            ffmpeg_cmd_reencode = [
                "ffmpeg",
                "-y",
                "-i", hls_url,
                "-t", str(duration_sec),
                "-c:v", "libx264",
                "-preset", "fast",
                "-c:a", "aac",
                "-movflags", "+faststart",
                output_path,
            ]
            ff_proc2 = subprocess.run(
                ffmpeg_cmd_reencode,
                capture_output=True,
                text=True,
                timeout=duration_sec + 120,
            )
            if ff_proc2.returncode != 0:
                return {
                    "error": f"ffmpeg falló: {ff_proc2.stderr[-300:]}",
                    "path": None,
                }

        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            if file_size < 1024:  # Menos de 1KB = probablemente vacío
                os.remove(output_path)
                return {"error": "Archivo capturado demasiado pequeño (stream no disponible?)", "path": None}

            return {
                "path": output_path,
                "filename": filename,
                "size_bytes": file_size,
                "size_mb": round(file_size / (1024 * 1024), 2),
                "duration_sec": duration_sec,
                "channel": channel_name,
                "method": "hls_capture",
                "error": None,
            }
        else:
            return {"error": "ffmpeg no produjo archivo de salida", "path": None}

    except subprocess.TimeoutExpired:
        return {"error": "Timeout capturando stream", "path": None}
    except Exception as e:
        return {"error": f"Error en captura HLS: {str(e)[:200]}", "path": None}
    finally:
        # Limpiar temp
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass


def get_best_clip(channel_name):
    """
    Obtiene el mejor clip disponible para un canal.
    Prioridad: API clips por views → captura HLS si no hay clips.
    """
    config = load_config()
    clip_cfg = config.get("clip_settings", {})
    min_dur = clip_cfg.get("min_duration_sec", 15)
    max_dur = clip_cfg.get("max_duration_sec", 60)
    min_views = clip_cfg.get("min_views_threshold", 50)

    # Intentar obtener clips via API
    api_result = get_clips_from_api(channel_name, sort_by="view_count", limit=20)

    if api_result.get("clips"):
        # Filtrar por duración aceptable
        valid_clips = [
            c for c in api_result["clips"]
            if min_dur <= c.get("duration", 0) <= max_dur
            and c.get("view_count", 0) >= min_views
            and c.get("video_url")
        ]

        if not valid_clips:
            # Relajar filtro de views
            valid_clips = [
                c for c in api_result["clips"]
                if c.get("duration", 0) <= max_dur
                and c.get("video_url")
            ]

        if valid_clips:
            best = valid_clips[0]
            download_result = download_clip(best)
            if download_result.get("path"):
                download_result["method"] = "api_clip"
                download_result["view_count"] = best.get("view_count", 0)
                return download_result

    # Fallback: captura HLS
    preferred_dur = clip_cfg.get("preferred_duration_sec", 30)
    return capture_hls_stream(channel_name, preferred_dur)


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    if len(args) < 1:
        print(json.dumps({
            "error": "Uso: python3 clip_extractor.py <canal> [--api|--capture <dur>|--best|--download <url>]"
        }))
        sys.exit(1)

    channel = args[0]
    mode = args[1] if len(args) > 1 else "--best"

    if mode == "--api":
        sort_by = args[2] if len(args) > 2 else "view_count"
        limit = int(args[3]) if len(args) > 3 else 10
        result = get_clips_from_api(channel, sort_by=sort_by, limit=limit)
        print(json.dumps(result, ensure_ascii=False))

    elif mode == "--capture":
        duration = int(args[2]) if len(args) > 2 else 30
        result = capture_hls_stream(channel, duration)
        print(json.dumps(result, ensure_ascii=False))

    elif mode == "--download":
        # Descargar un clip específico por URL
        if len(args) < 3:
            print(json.dumps({"error": "Falta URL del clip"}))
            sys.exit(1)
        clip_info = {
            "video_url": args[2],
            "channel": channel,
            "id": int(time.time()),
            "title": args[3] if len(args) > 3 else "",
        }
        result = download_clip(clip_info)
        print(json.dumps(result, ensure_ascii=False))

    elif mode == "--best":
        result = get_best_clip(channel)
        print(json.dumps(result, ensure_ascii=False))

    else:
        result = get_best_clip(channel)
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
