#!/usr/bin/env python3
"""
KickClipBot — parallel_uploader.py
Ejecuta múltiples uploads en paralelo usando ThreadPoolExecutor (máx 6)
"""

import sys
import os
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | parallel | %(message)s")
logger = logging.getLogger(__name__)

def run_single_clip(clip_id):
    """Ejecuta el pipeline para un clip específico"""
    import pipeline_worker
    try:
        logger.info(f"Iniciando upload para: {clip_id}")
        result = pipeline_worker.run_pipeline(clip_id)
        if result:
            logger.info(f"✅ {clip_id} subido exitosamente")
            return {"clip_id": clip_id, "status": "success"}
        else:
            logger.error(f"❌ {clip_id} falló")
            return {"clip_id": clip_id, "status": "failed"}
    except Exception as e:
        logger.error(f"❌ {clip_id} error: {e}")
        return {"clip_id": clip_id, "status": "error", "error": str(e)}

def main():
    # Importar state_manager para obtener clips pendientes
    sys.path.append(SCRIPT_DIR)
    from state_manager import StateManager
    
    db = StateManager()
    
    # Obtener clips pendientes (máx 6)
    pending_clips = db.get_pending_clips(limit=6)
    
    if not pending_clips:
        logger.info("No hay clips pendientes. Nada que procesar.")
        return
    
    logger.info(f"=== INICIANDO {len(pending_clips)} UPLOADS EN PARALELO ===")
    
    # Ejecutar en paralelo (máx 6 workers)
    with ThreadPoolExecutor(max_workers=min(len(pending_clips), 6)) as executor:
        # Submeter todas las tareas
        future_to_clip = {
            executor.submit(run_single_clip, clip["id"]): clip 
            for clip in pending_clips
        }
        
        # Recolectar resultados
        results = []
        for future in as_completed(future_to_clip):
            clip = future_to_clip[future]
            try:
                result = future.result()
                results.append(result)
                status = result.get("status", "unknown")
                logger.info(f"Resultado {clip['id']}: {status}")
            except Exception as e:
                logger.error(f"Excepción en {clip['id']}: {e}")
                results.append({"clip_id": clip["id"], "status": "error", "error": str(e)})
    
    # Resumen
    success = sum(1 for r in results if r.get("status") == "success")
    failed = sum(1 for r in results if r.get("status") in ["failed", "error"])
    
    logger.info(f"=== RESUMEN: {success} exitosos, {failed} fallidos ===")

if __name__ == "__main__":
    main()