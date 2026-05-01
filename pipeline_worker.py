#!/usr/bin/env python3
"""
KickClipBot — pipeline_worker.py (Senior Engineer Edition)
═══════════════════════════════════════════════════════════════
Ejecuta el ciclo de vida completo de un clip pendiente en la DB.
1. Extracción (hls_extractor)
2. Procesamiento (video_processor + whisper)
3. Generación de Payload (publish_optimizer)
4. Subida (tiktok_uploader)
5. Limpieza y marcado en DB
"""

import sys
import os
import json
import logging
import traceback

# Importar los submódulos
import hls_extractor
import video_processor
import publish_optimizer
from tiktok_uploader import TikTokUploader
from state_manager import StateManager

# Cargar configuración
def load_config():
    with open("config.json", "r") as f:
        return json.load(f)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | pipeline | %(message)s")
logger = logging.getLogger(__name__)

def run_pipeline(clip_id):
    logger.info(f"=== Iniciando Pipeline para Clip ID: {clip_id} ===")
    db = StateManager()
    
    # 1. Obtener detalles del clip de la base de datos
    clips = db._get_conn().execute("SELECT * FROM clips WHERE id = ?", (clip_id,)).fetchall()
    if not clips:
        logger.error(f"Clip {clip_id} no encontrado en la base de datos.")
        return False
        
    clip = dict(clips[0])
    channel = clip["channel"]
    
    # Parsear Metadata
    try:
        metadata = json.loads(clip["metadata"])
    except Exception:
        metadata = {}
        
    source_url = metadata.get("source_url")
    start_sec = metadata.get("start_sec", 0)
    duration_sec = metadata.get("duration_sec", 30) # Fallback 30s
    
    if not source_url:
        logger.error("No se encontró 'source_url' (m3u8) en la metadata del clip.")
        db.update_status(clip_id, "FAILED", increment_retry=True)
        return False
        
    raw_video_path = None
    processed_video_path = None
    
    try:
        # ==========================================
        # PASO 1: EXTRAER CLIP (HLS)
        # ==========================================
        filename = f"raw_{clip_id}.mp4"
        logger.info(f"1. Extrayendo clip HLS -> {filename}")
        extract_res = hls_extractor.extract_clip(source_url, start_sec, duration_sec, filename)
        if not extract_res.get("success"):
            raise Exception(f"Extracción HLS fallida: {extract_res.get('error')}")
            
        raw_video_path = extract_res["path"]
        fingerprint = hls_extractor.compute_fingerprint(raw_video_path)
        if fingerprint and db.fingerprint_exists(fingerprint):
            logger.warning("Clip duplicado detectado por fingerprint. Eliminando archivo y marcando FAILED.")
            db.update_status(clip_id, "FAILED", increment_retry=True)
            if os.path.exists(raw_video_path):
                os.remove(raw_video_path)
            return False
        
        # Guardar fingerprint en DB
        try:
            conn = db._get_conn()
            conn.execute("UPDATE clips SET fingerprint = ? WHERE id = ?", (fingerprint, clip_id))
            conn.commit()
            conn.close()
        except Exception:
            pass
        
        # ==========================================
        # PASO 2: PROCESAR VIDEO (FFmpeg + Whisper)
        # ==========================================
        logger.info(f"2. Procesando video (Crop 9:16, Subtítulos ASR, Watermark)")
        process_res = video_processor.process_video(raw_video_path, channel)
        if not process_res.get("success"):
            raise Exception(f"Procesamiento fallido: {process_res.get('error')}")
            
        processed_video_path = process_res["output"]
        asr_quote = process_res.get("asr_quote", "")
        logger.info(f"-> ASR Quote obtenido: '{asr_quote[:50]}...'")
        
        # ==========================================
        # PASO 3: GENERAR METADATA / PAYLOAD TIKTOK
        # ==========================================
        logger.info("3. Generando metadata viral optimizada para TikTok")
        payload = publish_optimizer.generate_payload(channel, asr_quote)
        logger.info(f"-> Payload Generado:\n{json.dumps(payload, indent=2, ensure_ascii=False)}")
        
        # ==========================================
        # PASO 4: SUBIR A TIKTOK
        # ==========================================
        logger.info("4. Ejecutando TikTok Uploader vía Playwright...")
        
        # Leemos configuración
        import configparser
        with open("config.json", "r") as f:
            cfg = json.load(f)
            headless_mode = cfg.get("tiktok_settings", {}).get("headless_browser", True)
            
        uploader = TikTokUploader(headless=headless_mode)
        
        upload_success = uploader.upload(processed_video_path, payload)
        
        if not upload_success:
            # Si falló, intentar sin subtítulos
            logger.warning("Primera subida falló. Reintentando sin subtítulos...")
            config = load_config()
            config["video_settings"]["subtitles_enabled"] = False
            with open("config.json", "w") as f:
                json.dump(config, f, indent=2)
            
            # Reprocesar sin subtitles
            process_res = video_processor.process_video(raw_video_path, channel)
            if not process_res.get("success"):
                raise Exception("TikTok uploader retornó False en ambos intentos")
            
            processed_video_path = process_res["output"]
            upload_success = uploader.upload(processed_video_path, payload)
            
            if not upload_success:
                raise Exception("TikTok uploader retornó False (Captcha, error de UI)")
        
        # Extraer URL del video publicado si está disponible
        tiktok_video_url = f"https://www.tiktok.com/@tu_usuario/video/{clip_id}"
        
        logger.info("=== Pipeline Completado con Éxito ===")
        db.update_status(clip_id, "UPLOADED", tiktok_url=tiktok_video_url)
        
    except Exception as e:
        logger.error(f"PIPELINE ERROR: {str(e)}")
        logger.debug(traceback.format_exc())
        db.update_status(clip_id, "FAILED", increment_retry=True)
        return False
        
    finally:
        # ==========================================
        # PASO 5: LIMPIEZA
        # ==========================================
        logger.info("5. Limpieza de archivos de disco...")
        if raw_video_path and os.path.exists(raw_video_path):
            try: os.remove(raw_video_path)
            except Exception as e: logger.warning(f"No se pudo borrar {raw_video_path}: {e}")
            
        if processed_video_path and os.path.exists(processed_video_path):
            try: os.remove(processed_video_path)
            except Exception as e: logger.warning(f"No se pudo borrar {processed_video_path}: {e}")

    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 pipeline_worker.py <clip_id>")
        sys.exit(1)
        
    run_pipeline(sys.argv[1])
