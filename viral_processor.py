#!/usr/bin/env python3
"""
KickClipBot — viral_processor.py
═══════════════════════════════════════════════════════════════
Procesador de video optimizado para viralidad en TikTok.

Mejoras sobre video_processor.py:
  - Recorta primeros 2s muertos (directo a la acción)
  - Gancho en los primeros 3 segundos
  - Captions animados estilo CapCut (pop-in por palabra)
  - Transcripción automática con Whisper
  - Intro 0.5s con nombre del canal (fade-in)
  - Outro 1s con CTA "Sígueme para más clips" (fade-out)
  - 1080x1920, 60fps, H.264, AAC

Uso:
    python3 viral_processor.py <input.mp4> <canal> [--no-subs] [--no-intro]
    python3 viral_processor.py <input.mp4> <canal> --score 8.5

Salida: JSON con path al video procesado
"""

import json
import sys
import os
import subprocess
import shutil
import math
from datetime import datetime

# ─── Configuración ────────────────────────────────────────────────────────────

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
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
    processed = os.path.join(SCRIPT_DIR, paths.get("processed_dir", "./processed"))
    temp = os.path.join(SCRIPT_DIR, paths.get("temp_dir", "./temp"))
    os.makedirs(processed, exist_ok=True)
    os.makedirs(temp, exist_ok=True)
    return {"processed": processed, "temp": temp}


# ─── FFprobe ──────────────────────────────────────────────────────────────────

def get_video_info(input_path):
    """Obtiene info del video con ffprobe."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_format", "-show_streams", input_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            vs = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), None)
            aus = next((s for s in data.get("streams", []) if s.get("codec_type") == "audio"), None)
            duration = float(data.get("format", {}).get("duration", 0))
            width = int(vs.get("width", 0)) if vs else 0
            height = int(vs.get("height", 0)) if vs else 0
            fps_str = vs.get("r_frame_rate", "30/1") if vs else "30/1"
            try:
                num, den = fps_str.split("/")
                fps = round(int(num) / int(den), 2)
            except Exception:
                fps = 30
            return {
                "duration": duration, "width": width, "height": height,
                "fps": fps, "has_audio": aus is not None,
                "file_size": int(data.get("format", {}).get("size", 0)),
            }
        return {"error": f"ffprobe fail: {result.stderr[:200]}"}
    except Exception as e:
        return {"error": str(e)[:200]}


# ─── Transcripción con Whisper ────────────────────────────────────────────────

def transcribe_audio(input_path, temp_dir):
    """
    Transcribe audio con openai-whisper.
    Retorna lista de segmentos: [{start, end, text}, ...]
    """
    try:
        import whisper
    except ImportError:
        print("[viral_processor] whisper no instalado, omitiendo captions", file=sys.stderr)
        return []

    config = load_config()
    model_name = config.get("video_settings", {}).get("whisper_model", "base")

    try:
        print(f"[viral_processor] Transcribiendo con Whisper ({model_name})...", file=sys.stderr)
        model = whisper.load_model(model_name)
        result = model.transcribe(input_path, language=None, task="transcribe")
        segments = result.get("segments", [])
        print(f"[viral_processor] {len(segments)} segmentos transcritos", file=sys.stderr)
        return [
            {"start": s["start"], "end": s["end"], "text": s["text"].strip()}
            for s in segments if s.get("text", "").strip()
        ]
    except Exception as e:
        print(f"[viral_processor] Error Whisper: {str(e)[:200]}", file=sys.stderr)
        return []


# ─── Generador de Filtro ASS (Captions CapCut Style) ─────────────────────────

def generate_capcut_ass(segments, temp_dir, out_w=1080, out_h=1920):
    """
    Genera subtítulos estilo CapCut con pop-in por palabra.
    Fuente bold blanca, borde negro, posición centro-inferior, 72px.
    Animación: cada palabra aparece con efecto de escala (pop-in).
    """
    if not segments:
        return None

    ass_path = os.path.join(temp_dir, f"_capcut_{os.getpid()}.ass")

    header = f"""[Script Info]
Title: KickClipBot CapCut Captions
ScriptType: v4.00+
PlayResX: {out_w}
PlayResY: {out_h}
WrapStyle: 0
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Caption,Arial,72,&H00FFFFFF,&H000000FF,&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,4,2,2,40,40,200,1
Style: CaptionHighlight,Arial,78,&H0000BFFF,&H000000FF,&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,4,2,2,40,40,200,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""

    def fmt_time(sec):
        h = int(sec // 3600)
        m = int((sec % 3600) // 60)
        s = int(sec % 60)
        cs = int((sec % 1) * 100)
        return f"{h}:{m:02d}:{s:02d}.{cs:02d}"

    lines = []
    for seg in segments:
        start = seg["start"]
        end = seg["end"]
        text = seg["text"]

        # Dividir en palabras para efecto pop-in
        words = text.split()
        if not words:
            continue

        duration = end - start
        time_per_word = duration / len(words) if len(words) > 0 else duration

        # Mostrar palabras acumulativamente (efecto karaoke/pop-in)
        # Cada línea muestra todas las palabras hasta el momento actual
        for i, word in enumerate(words):
            word_start = start + (i * time_per_word)
            word_end = start + ((i + 1) * time_per_word)

            # Texto: palabras anteriores normales + palabra actual resaltada
            prev_words = " ".join(words[:i])
            current_word = word
            next_words = ""

            # Construir línea con la palabra actual en bold
            if prev_words:
                display = f"{prev_words} {{\\c&H00BFFF&\\fscx110\\fscy110}}{current_word}{{\\r}}"
            else:
                display = f"{{\\c&H00BFFF&\\fscx110\\fscy110}}{current_word}{{\\r}}"

            # Pop-in: escalar de 0% a 100% en 80ms
            pop_effect = "{\\fscx0\\fscy0\\t(0,80,\\fscx100\\fscy100)}"

            lines.append(
                f"Dialogue: 0,{fmt_time(word_start)},{fmt_time(word_end)},Caption,,0,0,0,,"
                f"{display}"
            )

        # Mostrar frase completa al final del segmento (último 30%)
        full_start = start + duration * 0.7
        full_text = " ".join(words)
        # Limitar a 2 líneas de ~35 chars
        if len(full_text) > 35:
            mid = len(words) // 2
            full_text = " ".join(words[:mid]) + "\\N" + " ".join(words[mid:])

        lines.append(
            f"Dialogue: 1,{fmt_time(full_start)},{fmt_time(end)},Caption,,0,0,0,,{full_text}"
        )

    with open(ass_path, "w", encoding="utf-8") as f:
        f.write(header)
        f.write("\n".join(lines))
        f.write("\n")

    return ass_path


# ─── Procesamiento Principal ─────────────────────────────────────────────────

def process_viral_video(input_path, channel_name, viral_score=0,
                        enable_subs=True, enable_intro=True):
    """
    Pipeline completo de procesamiento viral:
    1. Recortar primeros 2s muertos
    2. Escalar a 1080x1920 (9:16) con blur de fondo si es horizontal
    3. Intro 0.5s con nombre del canal (fade-in)
    4. Captions CapCut style con Whisper
    5. Watermark @canal
    6. Outro 1s con CTA (fade-out)
    7. Encode H.264, 60fps, AAC
    """
    if not os.path.exists(input_path):
        return {"error": f"Archivo no encontrado: {input_path}", "path": None}

    if not shutil.which("ffmpeg"):
        return {"error": "ffmpeg no instalado", "path": None}

    config = load_config()
    video_cfg = config.get("video_settings", {})
    out_w = video_cfg.get("output_width", 1080)
    out_h = video_cfg.get("output_height", 1920)
    vbitrate = video_cfg.get("video_bitrate", "4M")
    abitrate = video_cfg.get("audio_bitrate", "192k")

    dirs = ensure_dirs()
    info = get_video_info(input_path)
    if info.get("error"):
        return {"error": info["error"], "path": None}

    orig_dur = info["duration"]
    orig_w = info["width"]
    orig_h = info["height"]
    has_audio = info["has_audio"]

    print(f"[viral_processor] Original: {orig_w}x{orig_h}, {orig_dur:.1f}s, audio={has_audio}", file=sys.stderr)

    # ── Paso 0: Recortar primeros 2s muertos ──
    skip_start = 2.0
    if orig_dur <= 10:
        skip_start = 0.5  # Videos muy cortos: solo 0.5s
    elif orig_dur <= 5:
        skip_start = 0  # Ultra corto: no recortar

    max_duration = 60.0  # Máximo TikTok
    usable_dur = min(orig_dur - skip_start, max_duration)

    # Reservar espacio para intro (0.5s) y outro (1s)
    intro_dur = 0.5 if enable_intro else 0
    outro_dur = 1.0 if enable_intro else 0
    content_dur = usable_dur  # El contenido real

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_filename = f"{channel_name}_viral_{timestamp}.mp4"
    output_path = os.path.join(dirs["processed"], out_filename)

    # ── Paso 1: Extraer segmento recortado (sin los 2s muertos) ──
    trimmed_path = os.path.join(dirs["temp"], f"_trimmed_{os.getpid()}.mp4")
    trim_cmd = [
        "ffmpeg", "-y",
        "-ss", str(skip_start),
        "-i", input_path,
        "-t", str(content_dur),
        "-c", "copy",
        "-avoid_negative_ts", "make_zero",
        trimmed_path,
    ]
    try:
        subprocess.run(trim_cmd, capture_output=True, text=True, timeout=60)
    except Exception:
        trimmed_path = input_path  # Fallback al original

    if not os.path.exists(trimmed_path) or os.path.getsize(trimmed_path) < 1024:
        trimmed_path = input_path

    # ── Paso 2: Transcribir audio para captions ──
    subs_path = None
    if enable_subs and has_audio:
        segments = transcribe_audio(trimmed_path, dirs["temp"])
        if segments:
            subs_path = generate_capcut_ass(segments, dirs["temp"], out_w, out_h)

    # ── Paso 3: Generar intro image (nombre del canal) ──
    intro_path = None
    if enable_intro:
        intro_path = _generate_intro_clip(channel_name, intro_dur, out_w, out_h, dirs["temp"])

    # ── Paso 4: Generar outro image (CTA) ──
    outro_path = None
    if enable_intro:
        outro_path = _generate_outro_clip(channel_name, outro_dur, out_w, out_h, dirs["temp"])

    # ── Paso 5: Construir comando FFmpeg completo ──
    try:
        result_path = _build_final_video(
            trimmed_path, intro_path, outro_path, subs_path,
            channel_name, output_path,
            out_w, out_h, content_dur, intro_dur, outro_dur,
            vbitrate, abitrate, has_audio, info,
            dirs["temp"],
        )

        if result_path and os.path.exists(result_path):
            out_info = get_video_info(result_path)
            fsize = os.path.getsize(result_path)
            result = {
                "path": result_path,
                "filename": out_filename,
                "size_bytes": fsize,
                "size_mb": round(fsize / (1024 * 1024), 2),
                "width": out_info.get("width", out_w),
                "height": out_info.get("height", out_h),
                "duration": out_info.get("duration", 0),
                "fps": out_info.get("fps", 60),
                "channel": channel_name,
                "viral_score": viral_score,
                "has_captions": subs_path is not None,
                "has_intro": intro_path is not None,
                "skip_start_sec": skip_start,
                "error": None,
            }
            print(f"[viral_processor] Listo: {out_filename} ({result['size_mb']}MB, {result['duration']:.1f}s)", file=sys.stderr)
            return result
        else:
            return {"error": "No se generó archivo de salida", "path": None}

    except Exception as e:
        # Fallback: procesamiento simplificado
        print(f"[viral_processor] Pipeline complejo falló, intentando simplificado: {str(e)[:100]}", file=sys.stderr)
        return _fallback_process(trimmed_path, channel_name, output_path, out_w, out_h, content_dur, vbitrate, abitrate, has_audio)

    finally:
        # Limpiar temporales
        for tmp in [trimmed_path, intro_path, outro_path, subs_path]:
            if tmp and tmp != input_path and os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception:
                    pass


def _generate_intro_clip(channel_name, duration, w, h, temp_dir):
    """Genera clip de intro de 0.5s con nombre del canal y fade-in."""
    intro_path = os.path.join(temp_dir, f"_intro_{os.getpid()}.mp4")
    # Fondo negro degradado con texto del canal
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi",
        "-i", f"color=c=black:s={w}x{h}:d={duration}:r=60",
        "-f", "lavfi",
        "-i", f"anullsrc=channel_layout=stereo:sample_rate=44100",
        "-t", str(duration),
        "-vf", (
            f"drawtext=text='@{channel_name}':"
            f"fontsize=90:fontcolor=white:borderw=3:bordercolor=black:"
            f"x=(w-text_w)/2:y=(h-text_h)/2-50:font=Arial,"
            f"drawtext=text='🔴 KICK CLIPS':"
            f"fontsize=48:fontcolor=white@0.8:borderw=2:bordercolor=black:"
            f"x=(w-text_w)/2:y=(h/2)+40:font=Arial,"
            f"fade=t=in:st=0:d={duration * 0.8}"
        ),
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "18",
        "-c:a", "aac", "-b:a", "128k",
        "-pix_fmt", "yuv420p",
        "-shortest",
        intro_path,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if proc.returncode == 0 and os.path.exists(intro_path):
            return intro_path
    except Exception as e:
        print(f"[viral_processor] Error generando intro: {str(e)[:100]}", file=sys.stderr)
    return None


def _generate_outro_clip(channel_name, duration, w, h, temp_dir):
    """Genera clip de outro de 1s con CTA y fade-out."""
    outro_path = os.path.join(temp_dir, f"_outro_{os.getpid()}.mp4")
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi",
        "-i", f"color=c=black:s={w}x{h}:d={duration}:r=60",
        "-f", "lavfi",
        "-i", f"anullsrc=channel_layout=stereo:sample_rate=44100",
        "-t", str(duration),
        "-vf", (
            f"drawtext=text='Sigueme para mas clips':"
            f"fontsize=64:fontcolor=white:borderw=3:bordercolor=black:"
            f"x=(w-text_w)/2:y=(h-text_h)/2-30:font=Arial,"
            f"drawtext=text='@{channel_name} en Kick':"
            f"fontsize=48:fontcolor=cyan:borderw=2:bordercolor=black:"
            f"x=(w-text_w)/2:y=(h/2)+50:font=Arial,"
            f"fade=t=out:st={duration * 0.3}:d={duration * 0.7}"
        ),
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "18",
        "-c:a", "aac", "-b:a", "128k",
        "-pix_fmt", "yuv420p",
        "-shortest",
        outro_path,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if proc.returncode == 0 and os.path.exists(outro_path):
            return outro_path
    except Exception as e:
        print(f"[viral_processor] Error generando outro: {str(e)[:100]}", file=sys.stderr)
    return None


def _build_final_video(trimmed_path, intro_path, outro_path, subs_path,
                       channel_name, output_path,
                       out_w, out_h, content_dur, intro_dur, outro_dur,
                       vbitrate, abitrate, has_audio, orig_info, temp_dir):
    """
    Construye el video final concatenando intro + contenido procesado + outro.
    """
    # Primero procesar el contenido principal a 9:16
    main_processed = os.path.join(temp_dir, f"_main_{os.getpid()}.mp4")
    is_horizontal = orig_info.get("width", 0) > orig_info.get("height", 0)

    # Filtro para el contenido principal
    vfilters = []

    if is_horizontal:
        # Fondo blur + video centrado
        vfilters.append(
            f"split[bg][fg];"
            f"[bg]scale={out_w}:{out_h}:force_original_aspect_ratio=increase,"
            f"crop={out_w}:{out_h},gblur=sigma=25[blurred];"
            f"[fg]scale={out_w}:-2:flags=lanczos[scaled];"
            f"[blurred][scaled]overlay=(W-w)/2:(H-h)/2-80[base]"
        )
    else:
        vfilters.append(
            f"scale={out_w}:{out_h}:force_original_aspect_ratio=decrease,"
            f"pad={out_w}:{out_h}:(ow-iw)/2:(oh-ih)/2:black[base]"
        )

    # Watermark
    vfilters.append(
        f"[base]drawtext=text='@{channel_name}':"
        f"fontsize=42:fontcolor=white@0.8:borderw=2:bordercolor=black@0.6:"
        f"x=(w-text_w)/2:y=70:font=Arial[wm]"
    )

    last_label = "wm"

    # Subtítulos CapCut
    if subs_path and os.path.exists(subs_path):
        escaped = subs_path.replace("\\", "/").replace(":", "\\:").replace("'", "'\\''")
        vfilters.append(f"[{last_label}]ass='{escaped}'[captioned]")
        last_label = "captioned"

    # Asegurar label final
    if last_label != "out":
        vfilters.append(f"[{last_label}]null[out]")

    filter_complex = ";".join(vfilters)

    main_cmd = [
        "ffmpeg", "-y",
        "-i", trimmed_path,
        "-t", str(min(content_dur, 58)),  # Dejar espacio para intro+outro
        "-filter_complex", filter_complex,
        "-map", "[out]",
    ]

    if has_audio:
        main_cmd.extend(["-map", "0:a?"])

    main_cmd.extend([
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "19",
        "-r", "60",
        "-c:a", "aac", "-b:a", abitrate, "-ar", "44100",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        main_processed,
    ])

    print("[viral_processor] Procesando contenido principal...", file=sys.stderr)
    proc = subprocess.run(main_cmd, capture_output=True, text=True, timeout=300)

    if proc.returncode != 0 or not os.path.exists(main_processed):
        # Fallback sin filtro complejo
        print("[viral_processor] Filtro complejo falló, simplificando...", file=sys.stderr)
        simple_cmd = [
            "ffmpeg", "-y", "-i", trimmed_path,
            "-t", str(min(content_dur, 58)),
            "-vf", f"scale={out_w}:{out_h}:force_original_aspect_ratio=decrease,pad={out_w}:{out_h}:(ow-iw)/2:(oh-ih)/2:black",
            "-c:v", "libx264", "-preset", "fast", "-crf", "20", "-r", "60",
            "-c:a", "aac", "-b:a", abitrate,
            "-pix_fmt", "yuv420p",
            main_processed,
        ]
        proc2 = subprocess.run(simple_cmd, capture_output=True, text=True, timeout=300)
        if proc2.returncode != 0:
            raise Exception(f"ffmpeg main fail: {proc2.stderr[-300:]}")

    # Concatenar intro + main + outro
    parts = []
    if intro_path and os.path.exists(intro_path):
        parts.append(intro_path)
    parts.append(main_processed)
    if outro_path and os.path.exists(outro_path):
        parts.append(outro_path)

    if len(parts) == 1:
        # Solo el main, mover directamente
        shutil.move(main_processed, output_path)
        return output_path

    # Crear concat file
    concat_file = os.path.join(temp_dir, f"_concat_{os.getpid()}.txt")
    with open(concat_file, "w") as f:
        for p in parts:
            f.write(f"file '{p}'\n")

    concat_cmd = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", concat_file,
        "-c:v", "libx264", "-preset", "medium", "-crf", "19",
        "-r", "60",
        "-c:a", "aac", "-b:a", abitrate,
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        output_path,
    ]

    print("[viral_processor] Concatenando intro + contenido + outro...", file=sys.stderr)
    proc3 = subprocess.run(concat_cmd, capture_output=True, text=True, timeout=120)

    # Limpiar
    for f_path in [main_processed, concat_file]:
        if os.path.exists(f_path):
            try:
                os.remove(f_path)
            except Exception:
                pass

    if proc3.returncode != 0:
        # Si concat falla, usar solo el main
        if os.path.exists(main_processed):
            shutil.move(main_processed, output_path)
        else:
            raise Exception(f"concat fail: {proc3.stderr[-200:]}")

    return output_path if os.path.exists(output_path) else None


def _fallback_process(input_path, channel_name, output_path, w, h, dur, vbitrate, abitrate, has_audio):
    """Procesamiento simplificado como fallback."""
    print("[viral_processor] Usando procesamiento fallback simplificado", file=sys.stderr)
    cmd = [
        "ffmpeg", "-y",
        "-ss", "1",
        "-i", input_path,
        "-t", str(min(dur, 59)),
        "-vf", (
            f"scale={w}:{h}:force_original_aspect_ratio=decrease,"
            f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2:black,"
            f"drawtext=text='@{channel_name}':fontsize=42:"
            f"fontcolor=white@0.8:borderw=2:bordercolor=black@0.6:"
            f"x=(w-text_w)/2:y=70:font=Arial"
        ),
        "-c:v", "libx264", "-preset", "fast", "-crf", "21", "-r", "60",
        "-c:a", "aac", "-b:a", abitrate,
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        output_path,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if proc.returncode == 0 and os.path.exists(output_path):
        fsize = os.path.getsize(output_path)
        info = get_video_info(output_path)
        return {
            "path": output_path,
            "filename": os.path.basename(output_path),
            "size_bytes": fsize,
            "size_mb": round(fsize / (1024 * 1024), 2),
            "duration": info.get("duration", 0),
            "channel": channel_name,
            "has_captions": False,
            "has_intro": False,
            "method": "fallback",
            "error": None,
        }
    return {"error": f"Fallback también falló: {proc.stderr[-200:]}", "path": None}


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    if len(args) < 2:
        print(json.dumps({
            "error": "Uso: python3 viral_processor.py <input.mp4> <canal> [--no-subs] [--no-intro] [--score N]"
        }))
        sys.exit(1)

    input_path = args[0]
    channel_name = args[1]
    enable_subs = "--no-subs" not in args
    enable_intro = "--no-intro" not in args
    viral_score = 0

    for i, a in enumerate(args):
        if a == "--score" and i + 1 < len(args):
            try:
                viral_score = float(args[i + 1])
            except ValueError:
                pass

    result = process_viral_video(
        input_path, channel_name,
        viral_score=viral_score,
        enable_subs=enable_subs,
        enable_intro=enable_intro,
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
