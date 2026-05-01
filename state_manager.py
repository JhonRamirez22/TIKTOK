#!/usr/bin/env python3
"""
KickClipBot — state_manager.py
═══════════════════════════════════════════════════════════════
Módulo de gestión de estado usando SQLite para evitar race conditions
y persistir el estado de la cola de procesamiento.

Estados soportados: PENDING → PROCESSING → UPLOADED | FAILED

Uso standalone para test:
    python3 state_manager.py --init
    python3 state_manager.py --status
"""

import sqlite3
import os
import sys
import json
import logging
from datetime import datetime, timezone, timedelta

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | state_manager | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")

def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error cargando config: {e}")
        return {}

def get_db_path():
    config = load_config()
    db_path = config.get("database", {}).get("path", "./data/state.db")
    full_path = os.path.join(SCRIPT_DIR, db_path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    return full_path

class StateManager:
    def __init__(self):
        self.db_path = get_db_path()
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Inicializa el schema de la base de datos si no existe."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS clips (
                        id TEXT PRIMARY KEY,
                        channel TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        score REAL DEFAULT 0.0,
                        status TEXT DEFAULT 'PENDING',
                        upload_ts TEXT,
                        tiktok_url TEXT,
                        retry_count INTEGER DEFAULT 0,
                        file_path TEXT,
                        source_type TEXT DEFAULT 'live',
                        start_sec INTEGER DEFAULT 1800,
                        duration_sec INTEGER DEFAULT 30,
                        metadata TEXT,
                        fingerprint TEXT
                    )
                ''')
                
                # Índice para consultas de quota diaria y estado
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON clips(status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_channel_ts ON clips(channel, timestamp)')
                cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_fingerprint ON clips(fingerprint)')
                
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error inicializando DB: {e}")
            raise

    def add_clip(self, clip_id, channel, score, file_path=None, metadata=None, fingerprint=None):
        """Agrega un nuevo clip a la cola en estado PENDING."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR IGNORE INTO clips (id, channel, timestamp, score, status, file_path, metadata, fingerprint)
                    VALUES (?, ?, ?, ?, 'PENDING', ?, ?, ?)
                ''', (clip_id, channel, datetime.now(timezone.utc).isoformat(), 
                      score, file_path, json.dumps(metadata) if metadata else "{}", fingerprint))
                conn.commit()
                if cursor.rowcount > 0:
                    logger.info(f"Clip {clip_id} agregado a la cola (Score: {score})")
                    return True
                return False
        except sqlite3.Error as e:
            logger.error(f"Error agregando clip {clip_id}: {e}")
            return False

    def queue_clip_direct(self, channel, status="QUEUED", source_type="live", source_url="", 
                      start_sec=1800, duration_sec=30, metadata=None):
        """Agrega un clip directamente a la cola desde el daemon."""
        try:
            import time
            import random
            
            clip_id = f"{channel}_{int(time.time())}_{random.randint(1000, 9999)}"
            
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO clips (id, channel, timestamp, score, status, file_path, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    clip_id,
                    channel,
                    datetime.now(timezone.utc).isoformat(),
                    5.0,  # default score
                    status,
                    source_url,
                    json.dumps(metadata) if metadata else "{}"
                ))
                conn.commit()
                logger.info(f"Clip {clip_id} encolado desde daemon")
                return clip_id
        except sqlite3.Error as e:
            logger.error(f"Error encolando clip: {e}")
            return None

    def fingerprint_exists(self, fingerprint):
        if not fingerprint:
            return False
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT 1 FROM clips WHERE fingerprint = ? LIMIT 1', (fingerprint,))
                return cursor.fetchone() is not None
        except sqlite3.Error:
            return False

    def get_pending_clips(self, limit=5):
        """Obtiene clips pendientes ordenados por score."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM clips 
                    WHERE status = 'PENDING' 
                    ORDER BY score DESC 
                    LIMIT ?
                ''', (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error consultando clips pendientes: {e}")
            return []

    def update_status(self, clip_id, status, tiktok_url=None, increment_retry=False):
        """Actualiza el estado de un clip y guarda URL si se subieron"""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                
                upload_ts = datetime.now(timezone.utc).isoformat() if status == 'UPLOADED' else None
                
                query = "UPDATE clips SET status = ?"
                params = [status]
                
                if upload_ts:
                    query += ", upload_ts = ?"
                    params.append(upload_ts)
                if tiktok_url:
                    query += ", tiktok_url = ?"
                    params.append(tiktok_url)
                if increment_retry:
                    query += ", retry_count = retry_count + 1"
                    
                query += " WHERE id = ?"
                params.append(clip_id)
                
                cursor.execute(query, tuple(params))
                conn.commit()
                logger.info(f"Clip {clip_id} actualizado a estado {status}")
                if tiktok_url:
                    logger.info(f"URL del video guardado: {tiktok_url}")
                return cursor.rowcount > 0
        except sqlite3.Error as e:
            logger.error(f"Error actualizando estado de {clip_id}: {e}")
            return False

    def get_daily_upload_count(self):
        """Retorna la cantidad de clips subidos exitosamente en las últimas 24h."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                # Calcular 24 horas atrás
                yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
                
                cursor.execute('''
                    SELECT COUNT(*) as count FROM clips 
                    WHERE status = 'UPLOADED' AND upload_ts >= ?
                ''', (yesterday,))
                row = cursor.fetchone()
                return row['count'] if row else 0
        except sqlite3.Error as e:
            logger.error(f"Error consultando cuota diaria: {e}")
            return 999  # Fallback seguro para evitar spam

    def get_channel_daily_count(self, channel):
        """Retorna clips subidos para un canal específico en las últimas 24h."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
                
                cursor.execute('''
                    SELECT COUNT(*) as count FROM clips 
                    WHERE status = 'UPLOADED' AND channel = ? AND upload_ts >= ?
                ''', (channel, yesterday))
                row = cursor.fetchone()
                return row['count'] if row else 0
        except sqlite3.Error as e:
            logger.error(f"Error consultando cuota por canal: {e}")
            return 999

    def get_failed_clips_for_retry(self, max_retries=3):
        """Obtiene clips fallidos que aún pueden ser reintentados."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM clips 
                    WHERE status = 'FAILED' AND retry_count < ?
                ''', (max_retries,))
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error obteniendo clips fallidos: {e}")
            return []

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="KickClipBot State Manager")
    parser.add_argument("--init", action="store_true", help="Inicializar base de datos")
    parser.add_argument("--status", action="store_true", help="Mostrar estado general de la cola")
    parser.add_argument("--add-test", type=str, help="Agregar clip de prueba (formato: channel,score)")
    
    args = parser.parse_args()
    
    manager = StateManager()
    
    if args.init:
        print(f"Base de datos inicializada en {manager.db_path}")
    
    elif args.status:
        try:
            with manager._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT status, COUNT(*) as count FROM clips GROUP BY status")
                rows = cursor.fetchall()
                print("\n=== ESTADO DE LA COLA ===")
                if not rows:
                    print("La base de datos está vacía.")
                for row in rows:
                    print(f"- {row['status']}: {row['count']} clips")
                
                print(f"\nUploads (24h): {manager.get_daily_upload_count()}")
        except Exception as e:
            print(f"Error consultando estado: {e}")
            
    elif args.add_test:
        try:
            channel, score = args.add_test.split(",")
            clip_id = f"test_{int(datetime.now().timestamp())}"
            manager.add_clip(clip_id, channel, float(score), "/dev/null")
            print(f"Clip de prueba agregado con ID: {clip_id}")
        except Exception as e:
            print(f"Error agregando clip de prueba: {e}. Uso: --add-test channel,score")
    else:
        parser.print_help()
