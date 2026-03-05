#!/usr/bin/env python3
"""
KickClipBot — video_processor.py
═══════════════════════════════════════════════════════════════
Procesa clips para formato TikTok:
  - Redimensiona a vertical 9:16 (1080x1920)
  - Añade watermark con nombre del canal
  - Genera subtítulos automáticos con Whisper
  - Recorta a máximo 60s

Uso:
    python3 video_processor.py <input.mp4> <canal> [--no-subs] [--no-watermark]

Salida: JSON con path al video procesado
"""

import json
import sys
import os
import subprocess
import shutil
import tempfile
from datetime import datetime

# ─── Configuración ────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")


def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def ensure_dirs():
    config = load_config()
    paths = config.get("paths", {})
    processed_dir = os.path.join(SCRIPT_DIR, paths.get("processed_dir", "./processed"))
    temp_dir = os.path.join(SCRIPT_DIR, paths.get("temp_dir", "./temp"))
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    return {"processed": processed_dir, "temp": temp_dir}


# ─── Utilidades FFmpeg ────────────────────────────────────────────────────────

def get_video_info(input_path):
    """Obtiene información del video con ffprobe."""
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            input_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            video_stream = None
            audio_stream = None
            for s in data.get("streams", []):
                if s.get("codec_type") == "video" and video_stream is None:
                    video_stream = s
                elif s.get("codec_type") == "audio" and audio_stream is None:
                    audio_stream = s

            duration = float(data.get("format", {}).get("duration", 0))
            width = int(video_stream.get("width", 0)) if video_stream else 0
            height = int(video_stream.get("height", 0)) if video_stream else 0

            return {
                "duration": duration,
                "width": width,
                "height": height,
                "has_audio": audio_stream is not None,
                "video_codec": video_stream.get("codec_name", "") if video_stream else "",
                "audio_codec": audio_stream.get("codec_name", "") if audio_stream else "",
                "file_size": int(data.get("format", {}).get("size", 0)),
            }
        return {"error": f"ffprobe falló: {result.stderr[:200]}"}
    except Exception as e:
        return {"error": f"Error ffprobe: {str(e)[:200]}"}


def generate_subtitles(input_path, temp_dir):
    """
    Genera subtítulos usando Whisper (openai-whisper).
    Retorna path al archivo .ass de subtítulos.
    """
    try:
        import whisper
    except ImportError:
        print("[video_processor] whisper no instalado, omitiendo subtítulos", file=sys.stderr)
        return None

    config = load_config()
    model_name = config.get("video_settings", {}).get("whisper_model", "base")

    try:
        print(f"[video_processor] Generando subtítulos con Whisper ({model_name})...", file=sys.stderr)
        model = whisper.load_model(model_name)
        result = model.transcribe(input_path, language=None, task="transcribe")
        segments = result.get("segments", [])

        if not segments:
            print("[video_processor] No se detectó habla en el audio", file=sys.stderr)
            return None

        # Generar archivo ASS para subtítulos con estilo
        ass_path = os.path.join(temp_dir, f"_subs_{os.getpid()}.ass")

        ass_header = """[Script Info]
Title: KickClipBot Subtitles
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,Arial,52,&H00FFFFFF,&H000000FF,&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,3,1,2,40,40,120,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""

        def format_ass_time(seconds):
            h = int(seconds // 3600)
            m = int((seconds % 3600) // 60)
            s = int(seconds % 60)
            cs = int((seconds % 1) * 100)
            return f"{h}:{m:02d}:{s:02d}.{cs:02d}"

        lines = []
        for seg in segments:
            start = format_ass_time(seg["start"])
            end = format_ass_time(seg["end"])
            text = seg["text"].strip().replace("\n", "\\N")
            # Máximo 2 líneas de ~40 chars cada una
            if len(text) > 40:
                words = text.split()
                mid = len(words) // 2
                line1 = " ".join(words[:mid])
                line2 = " ".join(words[mid:])
                text = f"{line1}\\N{line2}"
            lines.append(f"Dialogue: 0,{start},{end},Default,,0,0,0,,{text}")

        with open(ass_path, "w", encoding="utf-8") as f:
            f.write(ass_header)
            f.write("\n".join(lines))
            f.write("\n")

        print(f"[video_processor] Subtítulos generados: {len(segments)} segmentos", file=sys.stderr)
        return ass_path

    except Exception as e:
        print(f"[video_processor] Error generando subtítulos: {str(e)[:200]}", file=sys.stderr)
        return None


def process_video(input_path, channel_name, enable_subs=True, enable_watermark=True):
    """
    Procesa video completo:
    1. Redimensiona a 1080x1920 (vertical 9:16)
    2. Añade watermark con nombre del canal
    3. Añade subtítulos (opcional)
    4. Recorta a máx 60s
    """
    if not os.path.exists(input_path):
        return {"error": f"Archivo no encontrado: {input_path}", "path": None}

    if not shutil.which("ffmpeg"):
        return {"error": "ffmpeg no instalado", "path": None}

    config = load_config()
    video_cfg = config.get("video_settings", {})
    out_w = video_cfg.get("output_width", 1080)
    out_h = video_cfg.get("output_height", 1920)
    wm_font_size = video_cfg.get("watermark_font_size", 42)
    wm_opacity = video_cfg.get("watermark_opacity", 0.8)
    video_bitrate = video_cfg.get("video_bitrate", "4M")
    audio_bitrate = video_cfg.get("audio_bitrate", "192k")
    max_duration = 60

    dirs = ensure_dirs()

    # Obtener info del video original
    info = get_video_info(input_path)
    if info.get("error"):
        return {"error": info["error"], "path": None}

    orig_w = info["width"]
    orig_h = info["height"]
    orig_dur = info["duration"]

    print(f"[video_processor] Original: {orig_w}x{orig_h}, {orig_dur:.1f}s", file=sys.stderr)

    # Generar nombre de salida
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_filename = f"{channel_name}_tiktok_{timestamp}.mp4"
    output_path = os.path.join(dirs["processed"], out_filename)

    # ── Construir filtro FFmpeg ──

    # Paso 1: Escalar y rellenar a 9:16
    # Si el video es horizontal (16:9), lo convertimos a vertical con blur de fondo
    is_horizontal = orig_w > orig_h

    if is_horizontal:
        # Video horizontal → fondo blur + video centrado arriba
        filter_parts = [
            # Crear fondo blur expandido
            f"[0:v]scale={out_w}:{out_h}:force_original_aspect_ratio=increase,crop={out_w}:{out_h},gblur=sigma=30[bg]",
            # Escalar video principal para que quepa en ancho
            f"[0:v]scale={out_w}:-2:flags=lanczos[fg]",
            # Overlay centrado verticalmente (un poco arriba del centro)
            f"[bg][fg]overlay=(W-w)/2:(H-h)/2-100[scaled]",
        ]
        last_label = "scaled"
    else:
        # Video ya es vertical o cuadrado → escalar directo
        filter_parts = [
            f"[0:v]scale={out_w}:{out_h}:force_original_aspect_ratio=decrease,pad={out_w}:{out_h}:(ow-iw)/2:(oh-ih)/2:black[scaled]",
        ]
        last_label = "scaled"

    # Paso 2: Watermark (texto con nombre del canal)
    if enable_watermark:
        # Color blanco con sombra negra para visibilidad
        alpha_hex = format(int(wm_opacity * 255), '02x')
        wm_text = f"@{channel_name}"
        filter_parts.append(
            f"[{last_label}]drawtext="
            f"text='{wm_text}':"
            f"fontsize={wm_font_size}:"
            f"fontcolor=white@{wm_opacity}:"
            f"borderw=2:"
            f"bordercolor=black@0.6:"
            f"x=(w-text_w)/2:"
            f"y=80:"
            f"font=Arial"
            f"[wm]"
        )
        last_label = "wm"

    # Paso 3: Subtítulos
    subs_path = None
    if enable_subs:
        subs_path = generate_subtitles(input_path, dirs["temp"])
        if subs_path and os.path.exists(subs_path):
            # Escapar path para filtro ASS
            escaped_path = subs_path.replace("\\", "/").replace(":", "\\:")
            filter_parts.append(
                f"[{last_label}]ass='{escaped_path}'[final]"
            )
            last_label = "final"

    # Si el último label no es "final", renombrarlo
    if last_label != "final":
        # Modificar el último filtro para que salga como [final]
        last_filter = filter_parts[-1]
        old_label = f"[{last_label}]"
        if last_filter.endswith(old_label):
            filter_parts[-1] = last_filter[:-len(old_label)] + "[final]"
        else:
            filter_parts.append(f"[{last_label}]null[final]")
        last_label = "final"

    filter_complex = ";".join(filter_parts)

    # ── Construir comando FFmpeg ──
    ffmpeg_cmd = [
        "ffmpeg",
        "-y",
        "-i", input_path,
        "-t", str(min(orig_dur, max_duration)),
        "-filter_complex", filter_complex,
        "-map", f"[{last_label}]",
        "-map", "0:a?" if info["has_audio"] else "-an",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-crf", "23",
        "-b:v", video_bitrate,
        "-maxrate", video_bitrate,
        "-bufsize", str(int(video_bitrate.replace("M", "")) * 2) + "M" if "M" in video_bitrate else "8M",
        "-c:a", "aac",
        "-b:a", audio_bitrate,
        "-ar", "44100",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        "-metadata", f"title={channel_name} clip",
        "-metadata", "comment=KickClipBot",
        output_path,
    ]

    # Remover -map 0:a? si no hay audio
    if not info["has_audio"]:
        ffmpeg_cmd = [x for x in ffmpeg_cmd if x != "-map" or ffmpeg_cmd[ffmpeg_cmd.index(x)+1] != "0:a?"]
        # Rebuild clean
        clean_cmd = []
        skip_next = False
        for i, arg in enumerate(ffmpeg_cmd):
            if skip_next:
                skip_next = False
                continue
            if arg == "-map" and i + 1 < len(ffmpeg_cmd) and ffmpeg_cmd[i + 1] == "0:a?":
                skip_next = True
                continue
            clean_cmd.append(arg)
        ffmpeg_cmd = clean_cmd
        # Asegurar -an
        if "-an" not in ffmpeg_cmd:
            idx = ffmpeg_cmd.index("-c:v")
            ffmpeg_cmd.insert(idx, "-an")

    print(f"[video_processor] Procesando → {out_w}x{out_h}...", file=sys.stderr)

    try:
        proc = subprocess.run(
            ffmpeg_cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )

        if proc.returncode != 0:
            # Intentar sin subtítulos si falló
            if subs_path and enable_subs:
                print("[video_processor] Reintentando sin subtítulos...", file=sys.stderr)
                return process_video(input_path, channel_name, enable_subs=False, enable_watermark=enable_watermark)

            # Intentar versión simplificada
            print("[video_processor] Reintentando con filtro simplificado...", file=sys.stderr)
            simple_cmd = [
                "ffmpeg", "-y",
                "-i", input_path,
                "-t", str(min(orig_dur, max_duration)),
                "-vf", f"scale={out_w}:{out_h}:force_original_aspect_ratio=decrease,pad={out_w}:{out_h}:(ow-iw)/2:(oh-ih)/2:black",
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
                "-c:a", "aac", "-b:a", audio_bitrate,
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                output_path,
            ]
            proc2 = subprocess.run(simple_cmd, capture_output=True, text=True, timeout=300)
            if proc2.returncode != 0:
                return {
                    "error": f"ffmpeg falló: {proc2.stderr[-400:]}",
                    "path": None,
                }

        if os.path.exists(output_path):
            out_info = get_video_info(output_path)
            file_size = os.path.getsize(output_path)

            result = {
                "path": output_path,
                "filename": out_filename,
                "size_bytes": file_size,
                "size_mb": round(file_size / (1024 * 1024), 2),
                "width": out_info.get("width", out_w),
                "height": out_info.get("height", out_h),
                "duration": out_info.get("duration", 0),
                "channel": channel_name,
                "watermark": enable_watermark,
                "subtitles": enable_subs and subs_path is not None,
                "error": None,
            }

            print(f"[video_processor] Listo: {out_filename} ({result['size_mb']} MB, {result['duration']:.1f}s)", file=sys.stderr)
            return result
        else:
            return {"error": "ffmpeg no produjo archivo de salida", "path": None}

    except subprocess.TimeoutExpired:
        return {"error": "Timeout procesando video (>300s)", "path": None}
    except Exception as e:
        return {"error": f"Error procesando: {str(e)[:200]}", "path": None}
    finally:
        # Limpiar subtítulos temporales
        if subs_path and os.path.exists(subs_path):
            try:
                os.remove(subs_path)
            except Exception:
                pass


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    if len(args) < 2:
        print(json.dumps({
            "error": "Uso: python3 video_processor.py <input.mp4> <canal> [--no-subs] [--no-watermark]"
        }))
        sys.exit(1)

    input_path = args[0]
    channel_name = args[1]
    enable_subs = "--no-subs" not in args
    enable_watermark = "--no-watermark" not in args

    result = process_video(input_path, channel_name, enable_subs, enable_watermark)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
