#!/usr/bin/env python3
"""
KickClipBot — hls_extractor.py
═══════════════════════════════════════════════════════════════
Descarga selectiva de fragmentos HLS en lugar de VODs completos
usando ffmpeg. Esto ahorra gigabytes de descarga y mucho tiempo.
"""

import sys
import os
import json
import logging
import subprocess
import hashlib
import tempfile

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CLIPS_DIR = os.path.join(SCRIPT_DIR, "clips")
os.makedirs(CLIPS_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | hls_extractor | %(message)s")
logger = logging.getLogger(__name__)

def extract_clip(m3u8_url, start_sec, duration_sec, output_filename):
    """
    Descarga una porción exacta de un stream HLS.
    """
    output_path = os.path.join(CLIPS_DIR, output_filename)
    logger.info(f"Extrayendo clip de {duration_sec}s a partir del seg {start_sec}")
    
    # FFmpeg es muy eficiente para descargar porciones HLS usando -ss antes del input
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-ss", str(start_sec),
        "-i", m3u8_url,
        "-t", str(duration_sec),
        "-c", "copy",
        output_path
    ]
    
    try:
        subprocess.run(cmd, check=True)
        if os.path.exists(output_path) and os.path.getsize(output_path) > 10000:
            logger.info(f"Extracción completada: {output_path}")
            return {"success": True, "path": output_path}
        else:
            logger.error("La extracción falló (archivo muy pequeño o no existe)")
            return {"success": False, "error": "File too small or missing"}
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg error: {e}")
        return {"success": False, "error": str(e)}

def compute_fingerprint(video_path):
    """Genera fingerprint MD5 con el primer y último frame del video."""
    if not os.path.exists(video_path):
        return None

    with tempfile.TemporaryDirectory() as tmp:
        first_frame = os.path.join(tmp, "first.jpg")
        last_frame = os.path.join(tmp, "last.jpg")

        # Primer frame
        subprocess.run([
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-i", video_path,
            "-frames:v", "1",
            first_frame
        ], check=False)

        # Último frame: usamos ffprobe para duración
        try:
            probe = subprocess.run([
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1", video_path
            ], capture_output=True, text=True, check=False)
            duration = float(probe.stdout.strip())
        except Exception:
            duration = 0

        if duration > 1:
            subprocess.run([
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                "-ss", str(max(duration - 0.1, 0)),
                "-i", video_path,
                "-frames:v", "1",
                last_frame
            ], check=False)

        md5 = hashlib.md5()
        for frame in [first_frame, last_frame]:
            if os.path.exists(frame):
                with open(frame, "rb") as f:
                    md5.update(f.read())

        return md5.hexdigest() if md5.hexdigest() else None

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Uso: python3 hls_extractor.py <m3u8_url> <start_sec> <duration_sec> <output_filename>")
        sys.exit(1)
        
    url = sys.argv[1]
    start = float(sys.argv[2])
    duration = float(sys.argv[3])
    outfile = sys.argv[4]
    
    result = extract_clip(url, start, duration, outfile)
    print(json.dumps(result))
