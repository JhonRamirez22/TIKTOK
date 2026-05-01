#!/usr/bin/env python3
"""
KickClipBot — AUTO-DAEMON.PY
══════════════════════════════════════════════════════════════════════
Daemon principal que:
1. Monitorea TODOS los canales en paralelo
2. Detecta streams en vivo y clips virales
3. Los encola en la DB para procesamiento
4. Procesa y sube clips a TikTok automaticamente
5. Evita duplicados usando fingerprinting
6. Corre indefinidamente

Usage:
    python3 auto_daemon.py              # Modo normal
    python3 auto_daemon.py --test   # Modo test (no hace upload)
    python3 auto_daemon.py --once  # Solo una iteracion
"""

import sys
import os
import json
import time
import logging
import random
import signal
import threading
import traceback
from datetime import datetime
from pathlib import Path

# Agregar directorio actual al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Importar modulos del proyecto
import clip_monitor
import hls_extractor
import video_processor
import publish_optimizer
import state_manager
from tiktok_uploader import TikTokUploader

# Cargar configuracion
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)

# Configurar logging
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"auto_daemon_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

#FLAGS
TEST_MODE = "--test" in sys.argv
ONCE_MODE = "--once" in sys.argv
RUNNING = True


def signal_handler(signum, frame):
    global RUNNING
    logger.info("=== SHUTDOWN SIGNAL RECIBIDO ===")
    RUNNING = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class AutoDaemon:
    """Daemon principal de automatizacion."""
    
    def __init__(self):
        self.config = load_config()
        self.db = state_manager.StateManager()
        self.channels = self.config.get("channels", [])
        self.session = clip_monitor.create_session()
        self.uploader = None
        
        # Limites
        self.max_clips_per_hour = self.config.get("clip_settings", {}).get("max_clips_per_hour_per_channel", 5)
        self.check_interval = self.config.get("monitor_settings", {}).get("check_interval_sec", 30)
        self.upload_delay_min = self.config.get("tiktok_settings", {}).get("upload_delay_min_sec", 3)
        self.upload_delay_max = self.config.get("tiktok_settings", {}).get("upload_delay_max_sec", 10)
        self.min_duration = self.config.get("clip_settings", {}).get("min_duration_sec", 15)
        self.max_duration = self.config.get("clip_settings", {}).get("max_duration_sec", 60)
        
        # Contadores
        self.clips_processed_this_hour = {}
        self.last_reset_hour = datetime.now().hour
        
        logger.info(f"=== AUTO-DAEMON INICIADO ===")
        logger.info(f"Canales a monitorear: {self.channels}")
        logger.info(f"Intervalo de chequeo: {self.check_interval}s")
        logger.info(f"Test mode: {TEST_MODE}")
        
    def check_quota(self, channel):
        """Verifica si podemos procesar mas clips de este canal."""
        current_hour = datetime.now().hour
        
        # Reset hourly
        if current_hour != self.last_reset_hour:
            self.clips_processed_this_hour = {}
            self.last_reset_hour = current_hour
            
        processed = self.clips_processed_this_hour.get(channel, 0)
        return processed < self.max_clips_per_hour
    
    def mark_processed(self, channel):
        """Marca un clip procesado."""
        self.clips_processed_this_hour[channel] = self.clips_processed_this_hour.get(channel, 0) + 1
    
    def check_all_channels(self):
        """Checkea todos los canales por streams en vivo."""
        results = []
        
        for channel in self.channels:
            try:
                info = clip_monitor.get_channel_info(self.session, channel)
                if info.get("live"):
                    results.append({
                        "channel": channel,
                        "livestream": info,
                        "viewers": info.get("viewers", 0),
                        "title": info.get("title", "")
                    })
                    logger.info(f"[LIVE] {channel}: {info.get('viewers', 0)} viewers - {info.get('title', '')[:50]}")
                else:
                    logger.debug(f"[OFFLINE] {channel}")
            except Exception as e:
                logger.error(f"Error checking {channel}: {e}")
        
        return results
    
    def detect_viral_moments(self, channel, livestream_info):
        """Detecta momentos virales en un stream."""
        try:
            # Get channel info and check engagement
            info = clip_monitor.get_channel_info(self.session, channel)
            
            if not info.get("is_live"):
                return None
            
            # Check engagement if live
            channel_id = info.get("channel_id")
            chatroom_id = info.get("chatroom_id")
            
            if channel_id and chatroom_id:
                engagement = clip_monitor.check_engagement(
                    self.session, channel, channel_id, chatroom_id
                )
                viewers = engagement.get("viewers", 0)
                chat_rate = engagement.get("chat_rate_per_min", 0)
                eng_score = engagement.get("engagement_score", 0)
                
                viral_score = min(eng_score / 10, 10)
            else:
                viewers = info.get("viewers", 0)
                viral_score = viewers / 100
            
            min_score = self.config.get("limits", {}).get("min_viral_score", 5)
            
            if viral_score >= min_score:
                logger.info(f"[VIRAL] {channel}: score={viral_score:.1f}")
                return {
                    "channel": channel,
                    "livestream": livestream_info,
                    "viral_score": viral_score,
                    "start_sec": 1800,
                    "duration_sec": random.randint(self.min_duration, self.max_duration)
                }
            
        except Exception as e:
            logger.error(f"Error detecting viral for {channel}: {e}")
        
        return None
    
    def check_vods_recent(self, channel):
        """Checkea VODs recientes del canal."""
        try:
            vods = clip_monitor.get_recent_vods(self.session, channel, hours=48)
            
            if not vods:
                return []
            
            # Filtrar VODs no procesados
            new_clips = []
            for vod in vods:
                # Checkear si ya foi procesado
                existing = self.db._get_conn().execute(
                    "SELECT id FROM clips WHERE channel = ? AND status != 'UPLOADED' AND metadata LIKE ?",
                    (channel, f"%{vod.get('id')}%")
                ).fetchone()
                
                if not existing:
                    # Create clip entry
                    clip_data = {
                        "channel": channel,
                        "source_type": "vod",
                        "source_url": vod.get("url"),
                        "start_sec": vod.get("start_sec", 1800),  # Skip intro
                        "duration_sec": min(vod.get("duration", 60), self.max_duration),
                        "viral_score": vod.get("view_count", 0) / 100,
                        "metadata": json.dumps({
                            "vod_id": vod.get("id"),
                            "title": vod.get("title"),
                            "source_url": vod.get("url")
                        })
                    }
                    new_clips.append(clip_data)
            
            return new_clips
            
        except Exception as e:
            logger.error(f"Error checking VODs for {channel}: {e}")
        
        return []
    
    def queue_clip(self, channel, clip_info):
        """Encola un clip en la DB."""
        try:
            # Verificar quota
            if not self.check_quota(channel):
                logger.warning(f"[QUOTA] Max clips per hour reached for {channel}")
                return None
            
            # Generar ID unico
            clip_id = f"{channel}_{int(time.time())}_{random.randint(1000, 9999)}"
            
            # Metadata
            metadata = clip_info.get("metadata", {})
            metadata.update({
                "channel": channel,
                "source_type": clip_info.get("source_type", "live"),
                "source_url": clip_info.get("source_url"),
                "start_sec": clip_info.get("start_sec", 1800),
                "duration_sec": clip_info.get("duration_sec", 30),
                "viral_score": clip_info.get("viral_score", 5)
            })
            
            # Insertar en DB usando queue_clip_direct
            clip_id = self.db.queue_clip_direct(
                channel=channel,
                status="QUEUED",
                source_type=clip_info.get("source_type", "live"),
                source_url=clip_info.get("source_url", ""),
                start_sec=clip_info.get("start_sec", 1800),
                duration_sec=clip_info.get("duration_sec", 30),
                metadata=metadata
            )
            
            self.mark_processed(channel)
            
            logger.info(f"[QUEUED] Clip {clip_id} for {channel}")
            return clip_id
            
        except Exception as e:
            logger.error(f"Error queueing clip: {e}")
            return None
    
    def process_queued_clips(self):
        """Procesa clips encolados pendientes."""
        try:
            # Obtener clips en cola
            conn = self.db._get_conn()
            pending = conn.execute(
                "SELECT * FROM clips WHERE status = 'QUEUED' ORDER BY timestamp ASC LIMIT 10"
            ).fetchall()
            
            if not pending:
                return 0
            
            processed = 0
            
            for row in pending:
                clip = dict(row)
                clip_id = clip["id"]
                channel = clip["channel"]
                
                # Parsear metadata
                try:
                    metadata = json.loads(clip["metadata"])
                except:
                    metadata = {}
                
                source_url = metadata.get("source_url", clip.get("source_url"))
                start_sec = int(metadata.get("start_sec", clip.get("start_sec", 1800)))
                duration_sec = int(metadata.get("duration_sec", clip.get("duration_sec", 30)))
                
                if not source_url:
                    logger.warning(f"Clip {clip_id} sin source_url")
                    self.db.update_status(clip_id, "FAILED")
                    continue
                
                if TEST_MODE:
                    logger.info(f"[TEST] Would process clip {clip_id} for {channel}")
                    self.db.update_status(clip_id, "PROCESSING")
                    time.sleep(2)
                    self.db.update_status(clip_id, "TEST_COMPLETE")
                    processed += 1
                    continue
                
                # --- EXTRACCION ---
                self.db.update_status(clip_id, "EXTRACTING")
                filename = f"raw_{clip_id}.mp4"
                
                try:
                    extract_res = hls_extractor.extract_clip(source_url, start_sec, duration_sec, filename)
                    if not extract_res.get("success"):
                        raise Exception(extract_res.get("error", "Extraction failed"))
                    
                    raw_path = extract_res["path"]
                    
                    # Check fingerprint
                    fingerprint = hls_extractor.compute_fingerprint(raw_path)
                    if fingerprint and self.db.fingerprint_exists(fingerprint):
                        logger.warning(f"[DUPLICATE] Clip {clip_id} fingerprint already exists")
                        self.db.update_status(clip_id, "FAILED")
                        if os.path.exists(raw_path):
                            os.remove(raw_path)
                        continue
                    
                    # Save fingerprint
                    conn.execute("UPDATE clips SET fingerprint = ? WHERE id = ?", (fingerprint, clip_id))
                    conn.commit()
                    
                except Exception as e:
                    logger.error(f"[EXTRACT] Error {clip_id}: {e}")
                    self.db.update_status(clip_id, "FAILED")
                    continue
                
                # --- PROCESAMIENTO ---
                self.db.update_status(clip_id, "PROCESSING")
                
                try:
                    process_res = video_processor.process_video(raw_path, channel)
                    if not process_res.get("success"):
                        raise Exception(process_res.get("error", "Processing failed"))
                    
                    processed_path = process_res["output"]
                    asr_quote = process_res.get("asr_quote", "")
                    
                except Exception as e:
                    logger.error(f"[PROCESS] Error {clip_id}: {e}")
                    self.db.update_status(clip_id, "FAILED")
                    if os.path.exists(raw_path):
                        os.remove(raw_path)
                    continue
                
                # --- GENERAR PAYLOAD ---
                try:
                    payload = publish_optimizer.generate_payload(channel, asr_quote)
                    logger.info(f"[PAYLOAD] {clip_id}: {payload.get('title', '')[:60]}")
                except Exception as e:
                    logger.error(f"[PAYLOAD] Error {clip_id}: {e}")
                    payload = {
                        "title": f"@{channel} clip | #fyp #parati",
                        "hashtags": ["#fyp", "#parati"]
                    }
                
                # --- UPLOAD ---
                self.db.update_status(clip_id, "UPLOADING")
                
                try:
                    if not self.uploader:
                        headless = self.config.get("tiktok_settings", {}).get("headless_browser", True)
                        self.uploader = TikTokUploader(headless=headless)
                    
                    delay = random.randint(self.upload_delay_min, self.upload_delay_max)
                    logger.info(f"[UPLOAD] Waiting {delay}s before upload...")
                    time.sleep(delay)
                    
                    upload_success = self.uploader.upload(processed_path, payload)
                    
                    if not upload_success:
                        # Retry sin subtitles
                        logger.warning(f"[RETRY] Clip {clip_id} failed, retrying without subtitles...")
                        self.config["video_settings"]["subtitles_enabled"] = False
                        save_config(self.config)
                        
                        process_res = video_processor.process_video(raw_path, channel)
                        if not process_res.get("success"):
                            raise Exception("Retry processing failed")
                        
                        processed_path = process_res["output"]
                        upload_success = self.uploader.upload(processed_path, payload)
                    
                    if not upload_success:
                        raise Exception("Upload failed after retry")
                    
                    tiktok_url = f"https://www.tiktok.com/@tu_usuario/video/{clip_id}"
                    self.db.update_status(clip_id, "UPLOADED", tiktok_url=tiktok_url)
                    logger.info(f"[SUCCESS] Clip {clip_id} uploaded!")
                    
                except Exception as e:
                    logger.error(f"[UPLOAD] Error {clip_id}: {e}")
                    self.db.update_status(clip_id, "FAILED")
                
                # --- LIMPIEZA ---
                try:
                    if os.path.exists(raw_path):
                        os.remove(raw_path)
                    if os.path.exists(processed_path):
                        os.remove(processed_path)
                except:
                    pass
                
                processed += 1
                self.mark_processed(channel)
            
            return processed
            
        except Exception as e:
            logger.error(f"[PROCESS] Main loop error: {e}")
            return 0
    
    def run(self):
        """Loop principal."""
        logger.info("=== INICIANDO LOOP PRINCIPAL ===")
        
        iteration = 0
        
        while RUNNING:
            iteration += 1
            start_time = time.time()
            
            try:
                # 1. Checkear canales activos
                live_channels = self.check_all_channels()
                
                # 2. Detectar momentos virales y encolar
                queued_now = 0
                
                for live in live_channels:
                    channel = live["channel"]
                    
                    # Try live viral detection
                    viral = self.detect_viral_moments(channel, live.get("livestream"))
                    if viral:
                        clip_id = self.queue_clip(channel, {
                            "source_type": "live",
                            "source_url": f"https://kick.com/{channel}/hls/{channel}.m3u8",
                            "viral_score": viral.get("viral_score", 5),
                            "metadata": viral.get("livestream", {})
                        })
                        if clip_id:
                            queued_now += 1
                    
                    # Also check recent VODs
                    try:
                        vod_clips = self.check_vods_recent(channel)
                        for vc in vod_clips[:2]:  # Max 2 por canal
                            clip_id = self.queue_clip(channel, vc)
                            if clip_id:
                                queued_now += 1
                    except Exception as e:
                        logger.error(f"[VOD] Error for {channel}: {e}")
                
                logger.info(f"[ITER {iteration}] Live: {len(live_channels)}, Queued: {queued_now}")
                
                # 3. Procesar clips encolados
                processed = self.process_queued_clips()
                logger.info(f"[ITER {iteration}] Processed: {processed}")
                
                # 4. Cleanup hourly counters
                current_hour = datetime.now().hour
                if current_hour != self.last_reset_hour:
                    self.clips_processed_this_hour = {}
                    self.last_reset_hour = current_hour
                    logger.info("[CLEANUP] Hourly counters reset")
                
            except Exception as e:
                logger.error(f"[ITER {iteration}] ERROR: {e}")
                logger.debug(traceback.format_exc())
            
            # Sleep intervalo
            elapsed = time.time() - start_time
            sleep_time = max(1, self.check_interval - elapsed)
            
            if ONCE_MODE:
                logger.info("[ONCE] Una iteracion completada, saliendo")
                break
            
            logger.info(f"[SLEEP] Esperando {sleep_time:.1f}s...")
            time.sleep(sleep_time)
        
        logger.info("=== DAEMON DETENIDO ===")


def main():
    """Entry point."""
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print(__doc__)
        sys.exit(0)
    
    daemon = AutoDaemon()
    daemon.run()


if __name__ == "__main__":
    main()