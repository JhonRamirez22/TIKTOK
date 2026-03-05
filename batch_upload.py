#!/usr/bin/env python3
"""
batch_upload.py — Extrae 1 clip de cada canal (LIVE o VOD), procesa y sube a TikTok.
NUNCA usa clips pre-existentes. SIEMPRE corta del stream/VOD real.

Flujo por canal:
  1. clip_extractor.py → detecta si LIVE u OFFLINE → corta clip del stream/VOD
  2. video_processor.py → procesa a 9:16 TikTok (--no-subs para velocidad)
  3. tiktok_uploader.py → sube a TikTok con título emoji + hashtags
"""

import json
import sys
import os
import subprocess
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON = sys.executable

CHANNELS = ["lonche", "willito", "westcol", "lasapaaaaa", "chanty", "Juanferlamata"]


def run_py(script, args, timeout=180):
    """Ejecuta un script Python y retorna el JSON parseado."""
    cmd = [PYTHON, os.path.join(BASE_DIR, script)] + args
    print(f"\n{'─'*60}", file=sys.stderr)
    print(f"▶ {script} {' '.join(args)}", file=sys.stderr)

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=BASE_DIR,
        )

        # Mostrar stderr (logs)
        if proc.stderr:
            for line in proc.stderr.strip().split('\n')[-10:]:  # Últimas 10 líneas
                print(f"  {line}", file=sys.stderr)

        # Parsear stdout JSON
        stdout = proc.stdout.strip()
        if not stdout:
            return {"error": f"Sin output de {script}"}

        # Tomar la última línea JSON (puede haber prints extra)
        lines = stdout.strip().split('\n')
        for line in reversed(lines):
            line = line.strip()
            if line.startswith('{'):
                return json.loads(line)

        return json.loads(stdout)

    except subprocess.TimeoutExpired:
        return {"error": f"Timeout ({timeout}s) en {script}"}
    except json.JSONDecodeError as e:
        return {"error": f"JSON inválido de {script}: {str(e)[:100]}"}
    except Exception as e:
        return {"error": f"Error ejecutando {script}: {str(e)[:200]}"}


def process_channel(channel, index, total):
    """Procesa un canal: extraer → procesar → subir."""
    print(f"\n{'═'*60}", file=sys.stderr)
    print(f"📺 [{index}/{total}] Canal: {channel}", file=sys.stderr)
    print(f"{'═'*60}", file=sys.stderr)

    # ─── Paso 1: Extraer clip (LIVE o VOD) ─────────
    print(f"\n📥 Extrayendo clip de {channel} (stream/VOD)...", file=sys.stderr)
    clip_result = run_py("clip_extractor.py", [channel, "--best"], timeout=600)  # 10min para análisis de audio

    if clip_result.get("error"):
        print(f"❌ Error extrayendo: {clip_result['error']}", file=sys.stderr)
        return {"channel": channel, "success": False, "step": "extract", "error": clip_result["error"]}

    clip_path = clip_result.get("path")
    clip_mode = clip_result.get("mode", "?")
    clip_source = clip_result.get("source", "?")
    if not clip_path or not os.path.exists(clip_path):
        print(f"❌ No se obtuvo clip para {channel} (modo: {clip_mode})", file=sys.stderr)
        return {"channel": channel, "success": False, "step": "extract",
                "error": clip_result.get("error", "No clip path"), "mode": clip_mode}

    clip_size = os.path.getsize(clip_path) / (1024 * 1024)
    print(f"✅ Clip extraído [{clip_mode}/{clip_source}]: {os.path.basename(clip_path)} ({clip_size:.1f}MB)", file=sys.stderr)

    # ─── Paso 2: Procesar video (9:16 + watermark) ─────────
    print(f"\n🎬 Procesando video de {channel}...", file=sys.stderr)
    process_result = run_py("video_processor.py", [clip_path, channel, "--no-subs"], timeout=300)

    if process_result.get("error"):
        print(f"⚠️ Error procesando, usando clip original: {process_result['error'][:80]}", file=sys.stderr)
        processed_path = clip_path  # Fallback: usar clip sin procesar
    else:
        processed_path = process_result.get("path", clip_path)
        if not os.path.exists(processed_path):
            processed_path = clip_path

    print(f"✅ Video listo: {os.path.basename(processed_path)}", file=sys.stderr)

    # ─── Paso 3: Subir a TikTok ─────────
    stream_title = clip_result.get("title", "")
    print(f"\n📤 Subiendo a TikTok: {channel}...", file=sys.stderr)
    upload_result = run_py("tiktok_uploader.py", [processed_path, channel, stream_title], timeout=300)

    if upload_result.get("success"):
        print(f"🎉 ¡SUBIDO! {channel} → TikTok", file=sys.stderr)
        # ─── Paso 4: BORRAR clips del disco (siempre) ─────────
        _cleanup_files(clip_path, processed_path)
        return {"channel": channel, "success": True, "method": upload_result.get("method"), "title": upload_result.get("title", "")}
    else:
        error = upload_result.get("error", "Unknown")
        needs_login = upload_result.get("needs_manual_login", False)
        print(f"❌ Upload falló: {error[:80]}", file=sys.stderr)
        # BORRAR aunque falle
        _cleanup_files(clip_path, processed_path)
        return {"channel": channel, "success": False, "step": "upload", "error": error, "needs_login": needs_login}


def _cleanup_files(*paths):
    """Borrar archivos de clips/procesados del disco."""
    for p in paths:
        try:
            if p and os.path.exists(p):
                size = os.path.getsize(p) / (1024 * 1024)
                os.remove(p)
                print(f"🗑️ Borrado: {os.path.basename(p)} ({size:.1f}MB)", file=sys.stderr)
        except Exception as e:
            print(f"⚠️ No se pudo borrar {p}: {e}", file=sys.stderr)


def main():
    print("╔══════════════════════════════════════════════════════════╗", file=sys.stderr)
    print("║  BATCH UPLOAD — 1 clip por canal (LIVE/VOD) → TikTok   ║", file=sys.stderr)
    print("║  NUNCA usa clips API. SIEMPRE corta del stream real.    ║", file=sys.stderr)
    print("╚══════════════════════════════════════════════════════════╝", file=sys.stderr)

    results = []
    success_count = 0
    total = len(CHANNELS)

    for i, channel in enumerate(CHANNELS, 1):
        result = process_channel(channel, i, total)
        results.append(result)

        if result.get("success"):
            success_count += 1

        # Si necesita login, parar
        if result.get("needs_login"):
            print(f"\n🔑 Login requerido. Ejecuta: python3 tiktok_uploader.py --login", file=sys.stderr)
            break

        # Delay entre uploads (10s)
        if i < total and result.get("success"):
            delay = 10
            print(f"\n⏳ Esperando {delay}s antes del siguiente canal...", file=sys.stderr)
            time.sleep(delay)

    # Resumen
    print(f"\n{'═'*60}", file=sys.stderr)
    print(f"📊 RESUMEN: {success_count}/{total} subidos exitosamente", file=sys.stderr)
    for r in results:
        icon = "✅" if r.get("success") else "❌"
        info = r.get("method", r.get("error", "")[:40])
        print(f"   {icon} {r['channel']}: {info}", file=sys.stderr)
    print(f"{'═'*60}", file=sys.stderr)

    # Output JSON final
    print(json.dumps({
        "total": total,
        "success": success_count,
        "failed": total - success_count,
        "results": results,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
