// kick_clip_bot.js
/**
 * KickClipBot — Daemon & Orchestrator (Senior Engineer Edition)
 * ═══════════════════════════════════════════════════════════════
 * Orquestador principal 100% autónomo.
 * Conecta base de datos SQLite y levanta un Endpoint HTTP para monitorizar.
 */

const { spawnSync, spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const express = require('express');
const Database = require('better-sqlite3');

const SCRIPT_DIR = __dirname;
const CONFIG_PATH = path.join(SCRIPT_DIR, 'config.json');
let config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));

// Server de Monitoreo
const app = express();
const port = config.alerts_and_monitoring?.health_check_port || 8080;

app.get('/health', (req, res) => {
    try {
        const dbPath = path.join(SCRIPT_DIR, config.database?.path || './data/state.db');
        const db = new Database(dbPath, { readonly: true });
        
        const counts = db.prepare("SELECT status, COUNT(*) as count FROM clips GROUP BY status").all();
        const stats = { pending: 0, processing: 0, uploaded: 0, failed: 0 };
        
        counts.forEach(c => {
            stats[c.status.toLowerCase()] = c.count;
        });
        
        res.json({
            status: "OK",
            uptime: process.uptime(),
            queue_stats: stats,
            timestamp: new Date().toISOString()
        });
        db.close();
    } catch (e) {
        res.status(500).json({ status: "ERROR", error: e.message });
    }
});

app.listen(port, () => {
    console.log(`[DAEMON] Health Check Endpoint corriendo en el puerto ${port}`);
});

/**
 * Main Loop del Orquestador
 */
async function mainLoop() {
    console.log(`\n[DAEMON] Iniciando ciclo de escaneo... ${new Date().toISOString()}`);
    
    // 1. Ejecutar el Stream Detector para todos los canales
    try {
        console.log(`[DAEMON] Escaneando canales en vivo...`);
        const result = spawnSync('python3', ['stream_detector.py', '--scan'], { encoding: 'utf-8' });
        const states = JSON.parse(result.stdout);
        
        for (const channelState of states) {
            if (channelState.mode === 'OFFLINE' && channelState.video_id) {
                console.log(`[DAEMON] Procesando VOD offline para ${channelState.channel} (ID: ${channelState.video_id})`);
                spawnSync('python3', ['stream_detector.py', '--process', channelState.channel], { stdio: 'inherit' });
            } else if (channelState.mode === 'ONLINE') {
                console.log(`[DAEMON] Canal ${channelState.channel} está EN VIVO. El detector está monitorizando...`);
            }
        }
    } catch (e) {
        console.error(`[DAEMON] Fallo en el detector: ${e.message}`);
    }

    // 2. Procesar clips en la cola (PENDING)
    try {
        const dbPath = path.join(SCRIPT_DIR, config.database?.path || './data/state.db');
        const db = new Database(dbPath);
        
        const pendingClips = db.prepare("SELECT * FROM clips WHERE status = 'PENDING' ORDER BY score DESC LIMIT 3").all();
        
        if (pendingClips.length > 0) {
            for (const clip of pendingClips) {
                console.log(`[DAEMON] Procesando clip de la cola: ${clip.id} (Canal: ${clip.channel})`);

                db.prepare("UPDATE clips SET status = 'PROCESSING' WHERE id = ?").run(clip.id);
                console.log(`[DAEMON] Delegando ejecución a pipeline_worker.py para el clip ${clip.id}...`);

                const workerProcess = spawnSync('python3', ['pipeline_worker.py', clip.id], { stdio: 'inherit' });
                if (workerProcess.status === 0) {
                    console.log(`[DAEMON] Worker finalizó correctamente para clip ${clip.id}`);
                } else {
                    console.error(`[DAEMON] Worker falló (Exit Code: ${workerProcess.status}) para clip ${clip.id}`);
                }
            }
        }
        
        db.close();
    } catch (e) {
        console.error(`[DAEMON] Error procesando la cola: ${e.message}`);
    }
}

// Ejecutar cada 5 minutos
setInterval(mainLoop, 5 * 60 * 1000);
mainLoop();
