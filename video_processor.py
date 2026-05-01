#!/usr/bin/env python3
"""
KickClipBot — video_processor.py (Senior Engineer Edition)
═══════════════════════════════════════════════════════════════
Pipeline completo de procesamiento de video para TikTok.

Características:
 - 2-pass encoding (1080x1920, H.264, AAC 128k)
 - Whisper ASR (Generación de .srt y burn-in con estilo CapCut)
 - Fade in/out de 0.5s
 - Watermark (10% opacidad, bottom_right)
 - Clean EXIF metadata
"""

import os
import sys
import json
import logging
import subprocess
from datetime import datetime
import ffmpeg
from faster_whisper import WhisperModel

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
TEMP_DIR = os.path.join(SCRIPT_DIR, "temp")
PROCESSED_DIR = os.path.join(SCRIPT_DIR, "processed")
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | video | %(message)s")
logger = logging.getLogger(__name__)

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

config = load_config().get("video_settings", {})

def generate_srt(video_path, output_srt):
    """Genera archivo SRT usando Faster-Whisper localmente."""
    logger.info("Iniciando transcripción con faster-whisper...")
    model_size = config.get("whisper_model", "base")
    
    # Use CPU by default or fallback to fast configs
    model = WhisperModel(model_size, device="cpu", compute_type="int8")
    
    segments, info = model.transcribe(video_path, word_timestamps=True)
    
    full_text = []
    # Formatear SRT
    with open(output_srt, "w", encoding="utf-8") as srt:
        for i, segment in enumerate(segments, start=1):
            start = format_timestamp(segment.start)
            end = format_timestamp(segment.end)
            text = segment.text.strip().upper() # Estilo TikTok MAYÚSCULAS
            full_text.append(text)
            
            srt.write(f"{i}\n{start} --> {end}\n{text}\n\n")
    logger.info(f"Subtítulos guardados en {output_srt}")
    return " ".join(full_text)

def format_timestamp(seconds):
    """Convierte segundos a formato HH:MM:SS,mmm"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int(round((seconds - int(seconds)) * 1000))
    return f"{hours:02}:{minutes:02}:{secs:02},{millis:03}"

def get_duration(video_path):
    probe = ffmpeg.probe(video_path)
    return float(probe["format"]["duration"])

def process_video(input_path, channel, watermark_text=None):
    """
    Pipeline principal FFmpeg con 2-pass encoding, fades, subtitles y watermark.
    """
    logger.info(f"Procesando {input_path} para {channel}...")
    
    filename = os.path.basename(input_path).rsplit(".", 1)[0]
    out_path = os.path.join(PROCESSED_DIR, f"{filename}_tiktok.mp4")
    srt_path = os.path.join(TEMP_DIR, f"{filename}.srt")
    pass1_log = os.path.join(TEMP_DIR, f"{filename}_pass1")
    
    # 1. Transcripción ASR
    asr_quote = ""
    if config.get("subtitles_enabled", True):
        asr_quote = generate_srt(input_path, srt_path)
        
    duration = get_duration(input_path)
    fade_duration = config.get("intro_outro_fade_sec", 0.5)
    
    # 2. Configurar el grafo de filtros
    try:
        # Input Streams
        in_stream = ffmpeg.input(input_path)
        v = in_stream.video
        a = in_stream.audio
        
        # Filtros de Video: Scale -> Crop -> Fades -> Subtitles -> Watermark
        # a. 9:16 vertical crop (centrado)
        v = ffmpeg.filter(v, 'crop', 'ih*9/16', 'ih')
        v = ffmpeg.filter(v, 'scale', config.get("output_width", 1080), config.get("output_height", 1920))
        
        # b. Intro/Outro Fades
        v = ffmpeg.filter(v, 'fade', type='in', start_time=0, duration=fade_duration)
        v = ffmpeg.filter(v, 'fade', type='out', start_time=duration-fade_duration, duration=fade_duration)
        
        # c. Audio Fades
        a = ffmpeg.filter(a, 'afade', type='in', start_time=0, duration=fade_duration)
        a = ffmpeg.filter(a, 'afade', type='out', start_time=duration-fade_duration, duration=fade_duration)
        
        # d. Subtítulos Burn-in
        if config.get("subtitles_enabled", True):
            font = config.get("subtitle_font", "Impact")
            stroke_w = config.get("subtitle_stroke_width", 3)
            stroke_c = config.get("subtitle_stroke_color", "black")
            
            # Escape path for ffmpeg filter
            esc_srt = srt_path.replace("\\", "/").replace(":", "\\:")
            style = f"FontName={font},FontSize=24,PrimaryColour=&H00FFFFFF,OutlineColour=&H00{stroke_c},Outline={stroke_w},Shadow=0,Alignment=2,MarginV=150"
            v = ffmpeg.filter(v, 'subtitles', esc_srt, force_style=style)
            
        # e. Watermark Text
        if config.get("watermark_enabled", True):
            wm_text = f"@{channel}"
            opacity = config.get("watermark_opacity", 0.1)
            fontsize = config.get("watermark_font_size", 42)
            
            # Bottom Right
            wm_x = "w-tw-50"
            wm_y = "h-th-50"
            v = ffmpeg.filter(v, 'drawtext', text=wm_text, x=wm_x, y=wm_y, 
                              fontsize=fontsize, fontcolor=f"white@{opacity}")

        # Configuración de salida común
        output_args = {
            "vcodec": "libx264",
            "acodec": "aac",
            "b:v": config.get("video_bitrate", "4M"),
            "b:a": config.get("audio_bitrate", "128k"),
            "map_metadata": "-1", # Clean EXIF metadata
            "movflags": "+faststart",
            "y": None # Overwrite
        }
        
        logger.info("Ejecutando Pass 1...")
        ffmpeg.output(v, os.devnull, f="null", passlogfile=pass1_log, **{"pass": 1, **{k:v for k,v in output_args.items() if k not in ["acodec", "b:a", "movflags"]}}).run(quiet=True, overwrite_output=True)
        
        logger.info("Ejecutando Pass 2...")
        ffmpeg.output(v, a, out_path, passlogfile=pass1_log, **{"pass": 2, **output_args}).run(quiet=True, overwrite_output=True)
        
        logger.info(f"Video finalizado: {out_path}")
        return {"success": True, "output": out_path, "asr_quote": asr_quote}

    except ffmpeg.Error as e:
        logger.error(f"Fallo en procesamiento de video FFmpeg: {e.stderr.decode()}")
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python3 video_processor.py <video.mp4> <channel>")
        sys.exit(1)
        
    result = process_video(sys.argv[1], sys.argv[2])
    print(json.dumps(result))