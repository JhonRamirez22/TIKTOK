#!/usr/bin/env node
/**
 * KickClipBot v2.1 — Orquestador + Stream Detector + Viral + Monetización
 * ═══════════════════════════════════════════════════════════════════
 * Pipeline completo:
 *   stream_detect → (LIVE: viral_detect | VOD: clip_extract) → viral_process → publish_optimize → upload → monetize
 *
 * Módulos integrados:
 *   stream_detector.py   → Smart Stream Detector (PRIMERO al iniciar)
 *   clip_monitor.py      → Monitoreo de canales Kick
 *   viral_detector.py    → Detección de momentos virales (score 1-10)
 *   clip_extractor.py    → Extracción de clips via API/HLS
 *   viral_processor.py   → Procesamiento CapCut-style (intro, captions, outro)
 *   publish_optimizer.py → Optimización de horarios peak COL
 *   tiktok_uploader.py   → Upload a TikTok (Playwright/API)
 *   monetization.py      → Tracking de métricas y milestones
 *
 * USO: node kick_clip_bot.js
 * REQ: pip install curl_cffi streamlink ffmpeg-python openai-whisper playwright yt-dlp
 *      npm install ws axios
 *      brew install ffmpeg
 */

'use strict';

const { spawnSync } = require('child_process');
const path          = require('path');
const fs            = require('fs');
const os            = require('os');

// ─── Configuración ───────────────────────────────────────────────────────────

const CONFIG_PATH = path.join(__dirname, 'config.json');
let CONFIG = {};
try {
    CONFIG = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
} catch (e) {
    console.error('❌ No se pudo cargar config.json:', e.message);
    process.exit(1);
}

const CHANNELS          = CONFIG.channels || ['lonche', 'willito', 'westcol', 'lasapaaaaa', 'chanty', 'Juanferlamata'];
const CLIP_CFG          = CONFIG.clip_settings || {};
const TIKTOK_CFG        = CONFIG.tiktok_settings || {};
const MONITOR_CFG       = CONFIG.monitor_settings || {};
const PATHS_CFG         = CONFIG.paths || {};
const VIRAL_CFG         = CONFIG.viral_detection || {};
const LIMITS_CFG        = CONFIG.limits || {};
const DASHBOARD_REFRESH = (CONFIG.dashboard || {}).refresh_interval_ms || 10000;

const MAX_CLIPS_PER_HOUR   = CLIP_CFG.max_clips_per_hour_per_channel || 5;
const CHECK_INTERVAL       = (MONITOR_CFG.check_interval_sec || 30) * 1000;
const MAX_UPLOADS_PER_DAY  = LIMITS_CFG.clips_per_day || TIKTOK_CFG.max_uploads_per_day || 5;
const MIN_VIRAL_SCORE      = LIMITS_CFG.min_viral_score || VIRAL_CFG.min_viral_score || 7;
const CLIPS_PER_CHANNEL    = LIMITS_CFG.clips_per_channel || 2;
const MIN_HOURS_BETWEEN    = LIMITS_CFG.min_hours_between_posts || 2;
const RECHECK_INTERVAL     = 15 * 60 * 1000;  // Re-verificar offlines cada 15 min

// ─── Utilidades ──────────────────────────────────────────────────────────────

const PID_PREFIX = `_kcb2_${process.pid}_`;

function pad(s, n)  { return String(s).padEnd(n); }
function padL(s, n) { return String(s).padStart(n); }
function rand(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function writePy(name, code) {
    const p = path.join(os.tmpdir(), PID_PREFIX + name);
    fs.writeFileSync(p, code, 'utf8');
    return p;
}

function runPy(scriptPath, timeoutMs = 60000) {
    const result = spawnSync('python3', [scriptPath], {
        timeout:   timeoutMs,
        encoding:  'utf8',
        maxBuffer: 1024 * 1024 * 50,
        stdio:     ['ignore', 'pipe', 'inherit'],
    });
    if (result.error) throw result.error;
    return (result.stdout || '').trim();
}

function runPyScript(scriptName, args = [], timeoutMs = 120000) {
    const scriptPath = path.join(__dirname, scriptName);
    if (!fs.existsSync(scriptPath)) {
        throw new Error(`Script no encontrado: ${scriptPath}`);
    }
    const result = spawnSync('python3', [scriptPath, ...args], {
        timeout:   timeoutMs,
        encoding:  'utf8',
        maxBuffer: 1024 * 1024 * 50,
        stdio:     ['ignore', 'pipe', 'inherit'],
        cwd:       __dirname,
    });
    if (result.error) throw result.error;
    return (result.stdout || '').trim();
}

function ensureDirs() {
    const dirs = ['clips_dir', 'processed_dir', 'uploaded_dir', 'logs_dir', 'temp_dir', 'cookies_dir'];
    for (const key of dirs) {
        const d = path.join(__dirname, PATHS_CFG[key] || `./${key.replace('_dir', '')}`);
        if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
    }
}

function logToFile(message) {
    const logsDir = path.join(__dirname, PATHS_CFG.logs_dir || './logs');
    if (!fs.existsSync(logsDir)) fs.mkdirSync(logsDir, { recursive: true });
    const logFile = path.join(logsDir, `bot_${new Date().toISOString().split('T')[0]}.log`);
    const timestamp = new Date().toISOString();
    fs.appendFileSync(logFile, `[${timestamp}] ${message}\n`, 'utf8');
}

function colTime() {
    // Hora Colombia UTC-5
    const now = new Date();
    const col = new Date(now.getTime() - 5 * 60 * 60 * 1000 + now.getTimezoneOffset() * 60000);
    return col;
}

function colTimeStr() {
    const col = colTime();
    const h = String(col.getHours()).padStart(2, '0');
    const m = String(col.getMinutes()).padStart(2, '0');
    const s = String(col.getSeconds()).padStart(2, '0');
    return `${h}:${m}:${s}`;
}

function colDateStr() {
    const col = colTime();
    return col.toISOString().split('T')[0];
}

// ─── Clase Principal ─────────────────────────────────────────────────────────

class KickClipBot {
    constructor() {
        this.startTime  = Date.now();
        this.channels   = {};
        this.queue      = [];
        this.processing = false;
        this.history    = [];

        // Stream detector state
        this.streamModes = {};        // { channel: 'LIVE_MODE' | 'VOD_RECENT' | 'VOD_OLD' | 'SKIP' }
        this.streamPriorities = {};   // { channel: 1|2|3|99 }
        this.initialScanDone = false;
        this.vodQueue = [];           // Cola de VODs por procesar
        this.transitions = [];        // Transiciones LIVE↔OFFLINE recientes

        // Contadores globales
        this.stats = {
            checks:            0,
            channels_live:     0,
            clips_found:       0,
            clips_extracted:   0,
            clips_processed:   0,
            clips_uploaded:    0,
            clips_failed:      0,
            clips_queued:      0,
            clips_from_vod:    0,
            errors:            0,
            uploads_today:     0,
            uploads_today_date: colDateStr(),
            viral_detections:  0,
        };

        // Monetización cache (se refresca cada minuto)
        this.monetization = {
            followers:        0,
            total_views:      0,
            total_likes:      0,
            total_clips:      0,
            today_clips:      0,
            today_views:      0,
            best_clip:        { channel: 'N/A', views: 0, title: '' },
            next_milestone:   null,
            avg_engagement:   0,
            last_refresh:     0,
        };

        // Publish optimizer cache
        this.publishInfo = {
            next_slot:    'calculando...',
            can_publish:  false,
            today_count:  0,
            today_limit:  MAX_UPLOADS_PER_DAY,
            last_refresh: 0,
        };

        // Rate limiting por canal
        this.rateLimit = {};

        // Inicializar canales
        for (const ch of CHANNELS) {
            this.channels[ch] = {
                name:          ch,
                is_live:       false,
                viewers:       0,
                title:         '',
                category:      '',
                engagement:    0,
                viral_score:   0,
                last_check:    null,
                last_clip:     null,
                clips_today:   0,
                status:        '⏳ Escaneando...',
                mode:          'UNKNOWN',
                vod_info:      null,
            };
            this.rateLimit[ch] = { clips_this_hour: 0, hour_start: Date.now() };
            this.streamModes[ch] = 'UNKNOWN';
            this.streamPriorities[ch] = 99;
        }
    }

    // ─── Formateo ────────────────────────────────────────────────────────

    formatUptime() {
        const s = Math.floor((Date.now() - this.startTime) / 1000);
        const d = Math.floor(s / 86400);
        const h = String(Math.floor((s % 86400) / 3600)).padStart(2, '0');
        const m = String(Math.floor((s % 3600) / 60)).padStart(2, '0');
        const sec = String(s % 60).padStart(2, '0');
        return d > 0 ? `${d}d ${h}:${m}:${sec}` : `${h}:${m}:${sec}`;
    }

    memUsage() {
        return `${(process.memoryUsage().rss / 1024 / 1024).toFixed(1)} MB`;
    }

    timestamp() {
        return colTimeStr();
    }

    // ─── Rate Limiting ───────────────────────────────────────────────────

    canClipChannel(channel) {
        const rl = this.rateLimit[channel];
        if (!rl) return true;

        const now = Date.now();
        const hourMs = 60 * 60 * 1000;

        if (now - rl.hour_start >= hourMs) {
            rl.clips_this_hour = 0;
            rl.hour_start = now;
        }

        // Check clips_per_channel limit
        const chData = this.channels[channel];
        if (chData && chData.clips_today >= CLIPS_PER_CHANNEL) return false;

        return rl.clips_this_hour < MAX_CLIPS_PER_HOUR;
    }

    recordClip(channel) {
        if (!this.rateLimit[channel]) {
            this.rateLimit[channel] = { clips_this_hour: 0, hour_start: Date.now() };
        }
        this.rateLimit[channel].clips_this_hour++;
    }

    canUploadToday() {
        const today = colDateStr();
        if (this.stats.uploads_today_date !== today) {
            this.stats.uploads_today = 0;
            this.stats.uploads_today_date = today;
            // Reset clips_today por canal
            for (const ch of CHANNELS) {
                if (this.channels[ch]) this.channels[ch].clips_today = 0;
            }
        }
        return this.stats.uploads_today < MAX_UPLOADS_PER_DAY;
    }

    // ─── Refresh Monetization Data ───────────────────────────────────────

    refreshMonetization() {
        const now = Date.now();
        if (now - this.monetization.last_refresh < 60000) return; // cada 60s

        try {
            const output = runPyScript('monetization.py', ['--dashboard'], 15000);
            if (output) {
                const data = JSON.parse(output);
                this.monetization.followers    = data.followers || 0;
                this.monetization.total_views  = data.total_views || 0;
                this.monetization.total_likes  = data.total_likes || 0;
                this.monetization.total_clips  = data.total_clips_tracked || 0;
                this.monetization.today_clips  = data.today_clips || 0;
                this.monetization.today_views  = data.today_views || 0;
                this.monetization.avg_engagement = data.avg_engagement_rate || 0;
                this.monetization.best_clip    = data.best_clip || { channel: 'N/A', views: 0, title: '' };
                this.monetization.next_milestone = data.next_milestone || null;
            }
        } catch (e) {
            // Silencioso — no es crítico
        }

        this.monetization.last_refresh = now;
    }

    // ─── Refresh Publish Optimizer ───────────────────────────────────────

    refreshPublishInfo() {
        const now = Date.now();
        if (now - this.publishInfo.last_refresh < 30000) return; // cada 30s

        try {
            const output = runPyScript('publish_optimizer.py', ['--next-slot'], 10000);
            if (output) {
                const data = JSON.parse(output);
                this.publishInfo.next_slot   = data.next_slot_time || 'N/A';
                this.publishInfo.can_publish = data.can_publish_now || false;
            }
        } catch (e) { /* silencioso */ }

        try {
            const output = runPyScript('publish_optimizer.py', ['--stats'], 10000);
            if (output) {
                const data = JSON.parse(output);
                this.publishInfo.today_count = data.today_published || 0;
                this.publishInfo.today_limit = data.daily_limit || MAX_UPLOADS_PER_DAY;
            }
        } catch (e) { /* silencioso */ }

        this.publishInfo.last_refresh = now;
    }

    // ─── Paso 0: Smart Stream Detector (PRIMERO al iniciar) ────────────

    initialStartupScan() {
        /**
         * Ejecuta stream_detector.py al inicio del bot.
         * Clasifica cada canal en LIVE_MODE / VOD_RECENT / VOD_OLD / SKIP.
         * Muestra pantalla de arranque estilo dashboard.
         */
        try {
            const output = runPyScript('stream_detector.py', [], 120000);
            if (!output) {
                this.addHistory('⚠️ Stream detector sin datos');
                this.stats.errors++;
                return;
            }

            const screen = JSON.parse(output);
            const summary = screen.summary || {};
            const modes = screen.modes || {};
            const priorities = screen.priorities || {};
            const fullData = screen.full_data || [];

            // Actualizar estado de cada canal
            for (const chData of fullData) {
                const ch = chData.channel;
                if (!this.channels[ch]) continue;

                const mode = chData.mode || 'SKIP';
                this.streamModes[ch] = mode;
                this.streamPriorities[ch] = chData.priority || 99;
                this.channels[ch].mode = mode;

                if (chData.status === 'LIVE') {
                    this.channels[ch].is_live  = true;
                    this.channels[ch].viewers  = chData.viewers || 0;
                    this.channels[ch].title    = (chData.title || '').substring(0, 40);
                    this.channels[ch].category = (chData.category || '').substring(0, 20);
                    this.channels[ch].status   = `🔴 LIVE ${chData.viewers || 0}v`;
                } else if (chData.status === 'OFFLINE') {
                    this.channels[ch].is_live = false;
                    const last = chData.last_stream;
                    const timeAgo = last ? (last.time_ago || '') : '';
                    this.channels[ch].vod_info = last;

                    if (mode === 'VOD_RECENT') {
                        this.channels[ch].status = `📼 VOD ${timeAgo}`;
                        this.vodQueue.push({
                            channel: ch, priority: 2, vod_data: last,
                            recent_clips: chData.recent_clips || [],
                            status: 'pending',
                        });
                    } else if (mode === 'VOD_OLD') {
                        this.channels[ch].status = `📼 VOD antiguo ${timeAgo}`;
                        this.vodQueue.push({
                            channel: ch, priority: 3, vod_data: last,
                            recent_clips: chData.recent_clips || [],
                            status: 'pending',
                        });
                    } else {
                        this.channels[ch].status = '⚫ Sin contenido';
                    }
                } else {
                    this.channels[ch].status = `⚠️ ${(chData.error || '?').substring(0, 20)}`;
                }

                this.channels[ch].last_check = this.timestamp();
            }

            // Ordenar VOD queue por prioridad
            this.vodQueue.sort((a, b) => a.priority - b.priority);

            this.stats.channels_live = summary.live || 0;
            this.initialScanDone = true;

            // Display startup
            this.displayStartupScreen(screen);

        } catch (e) {
            this.stats.errors++;
            this.addHistory(`❌ Stream detector: ${e.message.split('\n')[0].substring(0, 50)}`);
            logToFile(`ERROR stream_detector: ${e.message}`);
        }
    }

    displayStartupScreen(screen) {
        const summary = screen.summary || {};
        const channels = screen.channels || [];

        const w = 60;
        const line = '═'.repeat(w);

        let channelLines = [];
        for (const c of channels) {
            const name = pad(c.channel, 15);
            const display = (c.display || '').substring(0, w - 20);
            channelLines.push(`║  ${name}${pad(display, w - 18)}║`);
        }

        const liveCount = summary.live || 0;
        const vodCount = (summary.vod_recent || 0) + (summary.vod_old || 0);
        const skipCount = summary.skip || 0;

        const output = [
            '',
            `╔${line}╗`,
            `║   🎬 KICK CLIP BOT — Analizando canales...  ${pad('', 13)}║`,
            `╠${line}╣`,
            `║  Verificando estado de ${pad(`${summary.total || 0} canales...`, w - 27)}║`,
            `╠${line}╣`,
            ...channelLines,
            `╠${line}╣`,
            `║  MODO LIVE  (${pad(`${liveCount} canales): análisis de picos activo`, w - 17)}║`,
            `║  MODO VOD   (${pad(`${vodCount} canales): extrayendo mejores clips`, w - 17)}║`,
            `║  SKIP       (${pad(`${skipCount} canales): sin contenido reciente`, w - 17)}║`,
            `╚${line}╝`,
            '',
        ];

        console.log(output.join('\n'));

        // Log
        if (liveCount > 0) {
            this.addHistory(`🔴 ${liveCount} canales EN VIVO → modo live`);
        }
        if (vodCount > 0) {
            this.addHistory(`📼 ${vodCount} canales OFFLINE → modo VOD`);
        }
        logToFile(`SCAN INICIAL: ${liveCount} live, ${vodCount} vod, ${skipCount} skip`);
    }

    recheckOfflines() {
        /**
         * Re-verifica canales OFFLINE cada 15 min.
         * Detecta transiciones LIVE↔OFFLINE.
         */
        try {
            const output = runPyScript('stream_detector.py', ['--recheck'], 60000);
            if (!output) return;

            const data = JSON.parse(output);
            const transitions = data.transitions || [];

            for (const t of transitions) {
                const ch = t.channel;
                if (!this.channels[ch]) continue;

                if (t.transition === 'WENT_LIVE') {
                    this.addHistory(`⚡ ${ch} acaba de ir en vivo!`);
                    logToFile(`TRANSICIÓN: ${ch} → LIVE`);
                    this.channels[ch].is_live = true;
                    this.channels[ch].viewers = t.viewers || 0;
                    this.channels[ch].status = `🔴 LIVE ${t.viewers || 0}v`;
                    this.channels[ch].mode = 'LIVE_MODE';
                    this.streamModes[ch] = 'LIVE_MODE';
                    this.streamPriorities[ch] = 1;

                    // Remover de vodQueue si estaba
                    this.vodQueue = this.vodQueue.filter(v => v.channel !== ch);

                } else if (t.transition === 'WENT_OFFLINE') {
                    this.addHistory(`📼 ${ch} terminó stream → analizando VOD`);
                    logToFile(`TRANSICIÓN: ${ch} → OFFLINE (VOD)`);
                    this.channels[ch].is_live = false;
                    this.channels[ch].status = '📼 Analizando VOD...';
                    this.channels[ch].mode = 'VOD_RECENT';
                    this.streamModes[ch] = 'VOD_RECENT';
                    this.streamPriorities[ch] = 2;

                    // Agregar a vodQueue
                    const chStatus = (t.data || {});
                    this.vodQueue.push({
                        channel: ch,
                        priority: 2,
                        vod_data: chStatus.last_stream || {},
                        recent_clips: chStatus.recent_clips || [],
                        status: 'pending',
                    });
                }

                this.transitions.push(t);
            }

            // Mantener últimas 20 transiciones
            if (this.transitions.length > 20) {
                this.transitions = this.transitions.slice(-20);
            }

        } catch (e) {
            this.stats.errors++;
        }
    }

    processVodChannel(vodItem) {
        /**
         * Procesa un canal en modo VOD: extrae clips del stream anterior.
         * Usa stream_detector.py --vod-analyze <canal>
         */
        const channel = vodItem.channel;

        if (!this.canClipChannel(channel)) return null;
        if (!this.canUploadToday()) return null;

        this.addHistory(`📼 VOD: extrayendo clips de ${channel}...`);
        if (this.channels[channel]) {
            this.channels[channel].status = '📼 Extrayendo VOD...';
        }

        try {
            const output = runPyScript('stream_detector.py', ['--vod-analyze', channel], 300000);
            if (!output) {
                this.addHistory(`⚠️ VOD ${channel}: sin datos`);
                return null;
            }

            const result = JSON.parse(output);
            const clips = result.clips || [];

            if (clips.length === 0) {
                this.addHistory(`⚫ VOD ${channel}: sin clips encontrados`);
                if (this.channels[channel]) {
                    this.channels[channel].status = '⚫ VOD sin clips';
                }
                return null;
            }

            this.stats.clips_from_vod += clips.length;
            this.addHistory(`✅ VOD ${channel}: ${clips.length} clips (${result.strategy_used || '?'})`);
            logToFile(`VOD: ${channel} → ${clips.length} clips via ${result.strategy_used}`);

            // Agregar clips a la cola principal para procesamiento
            for (const clip of clips) {
                if (clip.path) {
                    this.queue.push({
                        channel,
                        streamTitle: clip.title || vodItem.vod_data?.title || '',
                        viralScore: 0,
                        clipPath: clip.path,  // ya extraído
                        status: 'extracted',  // saltar extracción
                        source: 'vod',
                        addedAt: Date.now(),
                    });
                    this.stats.clips_extracted++;
                }
            }

            if (this.channels[channel]) {
                this.channels[channel].status = `📼 ${clips.length} clips extraídos`;
            }

            return result;

        } catch (e) {
            this.stats.errors++;
            this.addHistory(`❌ VOD ${channel}: ${e.message.split('\n')[0].substring(0, 40)}`);
            logToFile(`ERROR vod ${channel}: ${e.message}`);
            return null;
        }
    }

    // ─── Paso 1: Monitorear Canales ──────────────────────────────────────

    monitorChannels() {
        this.addHistory('🔍 Escaneando canales...');

        try {
            const output = runPyScript('clip_monitor.py', [], 60000);
            if (!output) {
                this.addHistory('⚠️ Monitor sin datos');
                this.stats.errors++;
                return [];
            }

            const results = JSON.parse(output);
            let liveChannels = [];

            for (const info of results) {
                const ch = info.channel;
                if (!this.channels[ch]) continue;

                this.channels[ch].is_live    = info.is_live || false;
                this.channels[ch].viewers    = info.viewers || 0;
                this.channels[ch].title      = (info.title || '').substring(0, 40);
                this.channels[ch].category   = (info.category || '').substring(0, 20);
                this.channels[ch].last_check = this.timestamp();

                if (info.is_live) {
                    this.channels[ch].status = `🔴 LIVE ${info.viewers}v`;
                    liveChannels.push(ch);
                } else {
                    this.channels[ch].status = '⚫ Offline';
                }

                if (info.error) {
                    this.channels[ch].status = `⚠️ ${String(info.error).substring(0, 20)}`;
                }
            }

            this.stats.channels_live = liveChannels.length;
            this.stats.checks++;

            if (liveChannels.length > 0) {
                this.addHistory(`✅ ${liveChannels.length} LIVE: ${liveChannels.join(', ')}`);
            } else {
                this.addHistory('⚫ Todos offline');
            }

            return liveChannels;

        } catch (e) {
            this.stats.errors++;
            this.addHistory(`❌ Monitor: ${e.message.split('\n')[0].substring(0, 50)}`);
            logToFile(`ERROR monitor: ${e.message}`);
            return [];
        }
    }

    // ─── Paso 2: Detección Viral ─────────────────────────────────────────

    detectViral(channel) {
        try {
            const output = runPyScript('viral_detector.py', ['--score', channel], 30000);
            if (!output) return null;

            const data = JSON.parse(output);
            const score = data.viral_score || 0;

            if (this.channels[channel]) {
                this.channels[channel].viral_score = score;
                this.channels[channel].engagement  = Math.round(score * 10);
            }

            if (score >= MIN_VIRAL_SCORE) {
                this.stats.viral_detections++;
                this.addHistory(`🔥 VIRAL ${channel}: score ${score.toFixed(1)}/10`);
            }

            return data;

        } catch (e) {
            this.stats.errors++;
            return null;
        }
    }

    // ─── Paso 2b: Engagement fallback ────────────────────────────────────

    checkEngagement(channel) {
        try {
            const output = runPyScript('clip_monitor.py', ['--engagement', channel], 30000);
            if (!output) return null;

            const data = JSON.parse(output);
            if (this.channels[channel]) {
                this.channels[channel].engagement = data.engagement_score || 0;
            }
            return data;

        } catch (e) {
            this.stats.errors++;
            return null;
        }
    }

    // ─── Paso 3: Extraer Clip ────────────────────────────────────────────

    extractClip(channel) {
        if (!this.canClipChannel(channel)) {
            this.addHistory(`⏳ Limit ${channel} (${this.channels[channel].clips_today}/${CLIPS_PER_CHANNEL}/día)`);
            return null;
        }

        this.addHistory(`📥 Extrayendo ${channel}...`);
        if (this.channels[channel]) {
            this.channels[channel].status = '📥 Extrayendo...';
        }

        try {
            const output = runPyScript('clip_extractor.py', [channel, '--best'], 600000);
            if (!output) {
                this.stats.errors++;
                return null;
            }

            const result = JSON.parse(output);

            if (result.error) {
                this.addHistory(`⚠️ ${channel}: ${String(result.error).substring(0, 50)}`);
                this.stats.clips_failed++;
                return null;
            }

            if (result.path) {
                this.stats.clips_extracted++;
                this.recordClip(channel);
                if (this.channels[channel]) {
                    this.channels[channel].last_clip = this.timestamp();
                    this.channels[channel].clips_today++;
                }
                const fname = result.filename || path.basename(result.path);
                this.addHistory(`✅ Clip: ${fname.substring(0, 35)} (${result.size_mb || '?'}MB)`);
                logToFile(`CLIP: ${channel} → ${result.path}`);
                return result;
            }

            return null;

        } catch (e) {
            this.stats.errors++;
            this.stats.clips_failed++;
            this.addHistory(`❌ Extract ${channel}: ${e.message.split('\n')[0].substring(0, 45)}`);
            logToFile(`ERROR extractor ${channel}: ${e.message}`);
            return null;
        }
    }

    // ─── Paso 4: Procesar Video (Viral Processor) ────────────────────────

    processVideo(clipResult, channel) {
        if (!clipResult || !clipResult.path) return null;

        this.addHistory(`🎬 Procesando CapCut-style ${channel}...`);
        if (this.channels[channel]) {
            this.channels[channel].status = '🎬 Procesando...';
        }

        try {
            // Usar viral_processor.py (CapCut-style) como primario
            const args = [clipResult.path, channel];
            let output;

            try {
                output = runPyScript('viral_processor.py', args, 300000);
            } catch (e) {
                // Fallback al procesador básico
                this.addHistory(`⚠️ Fallback a procesador básico`);
                output = runPyScript('video_processor.py', args, 300000);
            }

            if (!output) {
                this.stats.errors++;
                return null;
            }

            const result = JSON.parse(output);

            if (result.error) {
                this.addHistory(`⚠️ Process ${channel}: ${String(result.error).substring(0, 45)}`);
                this.stats.clips_failed++;
                return null;
            }

            if (result.path) {
                this.stats.clips_processed++;
                const dur = Math.round(result.duration || 0);
                this.addHistory(`✅ Video: ${result.filename || '?'} (${result.size_mb}MB, ${dur}s)`);
                logToFile(`PROC: ${channel} → ${result.path}`);
                return result;
            }

            return null;

        } catch (e) {
            this.stats.errors++;
            this.stats.clips_failed++;
            this.addHistory(`❌ Process ${channel}: ${e.message.split('\n')[0].substring(0, 45)}`);
            logToFile(`ERROR processor ${channel}: ${e.message}`);
            return null;
        }
    }

    // ─── Paso 5: Publish Optimizer — Decidir si publicar o encolar ───────

    shouldPublish(channel) {
        try {
            const output = runPyScript('publish_optimizer.py', ['--should-publish', channel], 15000);
            if (!output) return { action: 'publish' }; // default: publicar

            const data = JSON.parse(output);
            return data;

        } catch (e) {
            return { action: 'publish' };
        }
    }

    generateMetadata(channel) {
        try {
            const output = runPyScript('publish_optimizer.py', ['--metadata', channel], 10000);
            if (!output) return null;
            return JSON.parse(output);
        } catch (e) {
            return null;
        }
    }

    recordPublication(channel) {
        try {
            runPyScript('publish_optimizer.py', ['--record', channel], 10000);
        } catch (e) { /* silencioso */ }
    }

    // ─── Paso 6: Subir a TikTok ─────────────────────────────────────────

    uploadToTiktok(processedResult, channel, streamTitle = '') {
        if (!processedResult || !processedResult.path) return null;

        if (!this.canUploadToday()) {
            this.addHistory(`⏳ Límite diario: ${this.stats.uploads_today}/${MAX_UPLOADS_PER_DAY}`);
            return null;
        }

        this.addHistory(`📤 Upload TikTok: ${channel}...`);
        if (this.channels[channel]) {
            this.channels[channel].status = '📤 Subiendo...';
        }

        try {
            const args = [processedResult.path, channel];
            if (streamTitle) args.push(streamTitle);

            const output = runPyScript('tiktok_uploader.py', args, 300000);
            if (!output) {
                this.stats.errors++;
                return null;
            }

            const result = JSON.parse(output);

            if (result.success) {
                this.stats.clips_uploaded++;
                this.stats.uploads_today++;
                this.addHistory(`🎉 ¡SUBIDO! ${channel} → TikTok (${result.method})`);
                logToFile(`UPLOAD: ${channel} → TikTok via ${result.method}`);
                if (this.channels[channel]) {
                    this.channels[channel].status = `✅ Subido ${this.timestamp()}`;
                }

                // Registrar publicación en optimizer
                this.recordPublication(channel);

                return result;
            }

            if (result.needs_manual_login) {
                this.addHistory(`🔑 TikTok: login manual requerido`);
            } else {
                this.addHistory(`⚠️ Upload: ${String(result.error || '?').substring(0, 45)}`);
            }
            this.stats.clips_failed++;
            return result;

        } catch (e) {
            this.stats.errors++;
            this.stats.clips_failed++;
            this.addHistory(`❌ Upload ${channel}: ${e.message.split('\n')[0].substring(0, 45)}`);
            logToFile(`ERROR upload ${channel}: ${e.message}`);
            return null;
        }
    }

    // ─── Paso 7: Registrar Métricas (Monetización) ──────────────────────

    recordMetrics(channel, viralScore = 0, title = '') {
        try {
            const args = [
                '--record-clip',
                channel,
                '0',                  // views (se actualizará después)
                '0',                  // likes
                '0',                  // comments
                '0',                  // shares
                title.substring(0, 80),
                String(viralScore),
            ];
            runPyScript('monetization.py', args, 10000);
        } catch (e) { /* silencioso */ }
    }

    // ─── Cola de Procesamiento ───────────────────────────────────────────

    addToQueue(channel, streamTitle = '', viralScore = 0) {
        // Evitar duplicados
        if (this.queue.find(q => q.channel === channel && q.status === 'pending')) {
            return;
        }
        this.queue.push({
            channel,
            streamTitle,
            viralScore,
            status: 'pending',
            addedAt: Date.now(),
        });
        this.stats.clips_found++;
    }

    async processQueue() {
        if (this.processing) return;
        if (this.queue.length === 0) return;

        this.processing = true;

        // Pick extracted (VOD) items first (higher prio), then pending
        const item = this.queue.find(q => q.status === 'extracted') || this.queue.find(q => q.status === 'pending');
        if (!item) {
            this.processing = false;
            return;
        }

        item.status = 'processing';

        try {
            let clipResult;

            // Si viene de VOD con path ya extraído, saltar extracción
            if (item.source === 'vod' && item.clipPath) {
                clipResult = {
                    path: item.clipPath,
                    filename: path.basename(item.clipPath),
                    size_mb: 0,
                };
                this.addHistory(`📼 VOD clip: ${path.basename(item.clipPath).substring(0, 35)}`);
            } else {
                // ── Paso 3: Extraer clip ──
                clipResult = this.extractClip(item.channel);
                if (!clipResult) {
                    item.status = 'failed';
                    this.processing = false;
                    return;
                }
            }

            await sleep(rand(2000, 5000));

            // ── Paso 4: Procesar video (CapCut-style) ──
            const processedResult = this.processVideo(clipResult, item.channel);
            if (!processedResult) {
                item.status = 'failed';
                this.processing = false;
                return;
            }

            // ── Paso 5: Consultar publish optimizer ──
            const publishDecision = this.shouldPublish(item.channel);

            if (publishDecision.action === 'queue') {
                // Encolar para horario peak
                item.status = 'queued';
                item.processedPath = processedResult.path;
                item.queuedUntil = publishDecision.next_slot_time || 'próximo peak';
                this.stats.clips_queued++;
                this.addHistory(`📅 Encolado ${item.channel} → ${item.queuedUntil}`);
                this.processing = false;
                return;
            }

            // ── Delay antes de upload ──
            const minDelay = rand(30, 60) * 1000;
            this.addHistory(`⏳ Upload en ${Math.round(minDelay / 1000)}s...`);
            await sleep(minDelay);

            // ── Paso 6: Subir a TikTok ──
            const uploadResult = this.uploadToTiktok(processedResult, item.channel, item.streamTitle);

            if (uploadResult && uploadResult.success) {
                item.status = 'uploaded';

                // ── Paso 7: Registrar métricas ──
                this.recordMetrics(item.channel, item.viralScore, item.streamTitle);
            } else {
                item.status = 'failed';
            }

        } catch (e) {
            item.status = 'failed';
            this.stats.errors++;
            this.addHistory(`❌ Pipeline: ${e.message.split('\n')[0].substring(0, 45)}`);
        }

        // Limpiar cola (mantener últimos 30)
        const active = this.queue.filter(q => ['pending', 'processing', 'queued', 'extracted'].includes(q.status));
        const done   = this.queue.filter(q => q.status === 'uploaded' || q.status === 'failed').slice(-30);
        this.queue = [...active, ...done];

        this.processing = false;
    }

    // Procesar items encolados cuyo horario peak ya llegó
    async processQueuedItems() {
        const queuedItems = this.queue.filter(q => q.status === 'queued' && q.processedPath);
        if (queuedItems.length === 0) return;

        for (const item of queuedItems) {
            const decision = this.shouldPublish(item.channel);
            if (decision.action === 'publish') {
                item.status = 'processing';
                this.addHistory(`🕐 Horario peak → subiendo ${item.channel}`);

                const fakeProcessed = { path: item.processedPath };
                const uploadResult = this.uploadToTiktok(fakeProcessed, item.channel, item.streamTitle);

                if (uploadResult && uploadResult.success) {
                    item.status = 'uploaded';
                    this.recordMetrics(item.channel, item.viralScore, item.streamTitle);
                } else {
                    item.status = 'failed';
                }

                await sleep(rand(5000, 15000));
            }
        }
    }

    // ─── Historial ───────────────────────────────────────────────────────

    addHistory(msg) {
        const entry = `[${this.timestamp()}] ${msg}`;
        this.history.push(entry);
        if (this.history.length > 20) this.history.shift();
    }

    // ─── Dashboard v2.0 ─────────────────────────────────────────────────

    updateDashboard() {
        const today = colDateStr();
        if (this.stats.uploads_today_date !== today) {
            this.stats.uploads_today = 0;
            this.stats.uploads_today_date = today;
        }

        // Refresh datos externos
        this.refreshMonetization();
        this.refreshPublishInfo();

        const w = 66;
        const line = '═'.repeat(w);
        const thinLine = '─'.repeat(w);

        // ── Canales ──
        let channelLines = [];
        for (const ch of CHANNELS) {
            const c = this.channels[ch] || {};
            const mode = this.streamModes[ch] || 'UNKNOWN';
            const modeIcon = mode === 'LIVE_MODE' ? '🔴' : mode === 'VOD_RECENT' ? '📼' : mode === 'VOD_OLD' ? '📁' : '⚫';
            const viewers = c.is_live ? `${c.viewers}v` : '';
            const vs = c.viral_score > 0 ? `🔥${c.viral_score.toFixed(1)}` : '';
            const clips = c.clips_today > 0 ? `📎${c.clips_today}` : '';
            const status = (c.status || '').substring(0, 22);
            const info = [viewers, vs, clips].filter(Boolean).join(' ');

            channelLines.push(
                `║ ${modeIcon} ${pad(ch, 14)} ${pad(info, 18)} ${pad(status, 26)}║`
            );
        }

        // ── Cola ──
        const pending  = this.queue.filter(q => q.status === 'pending').length;
        const procNow  = this.queue.filter(q => q.status === 'processing').length;
        const queued   = this.queue.filter(q => q.status === 'queued').length;
        const uploaded = this.queue.filter(q => q.status === 'uploaded').length;
        const failed   = this.queue.filter(q => q.status === 'failed').length;

        // ── Monetización ──
        const mon = this.monetization;
        const followers = mon.followers;
        const bestClip  = mon.best_clip || { channel: 'N/A', views: 0, title: '' };
        const nextMs    = mon.next_milestone;

        // ── Publish ──
        const pub = this.publishInfo;

        // ── Historial (últimas 7 líneas) ──
        const histLines = this.history.slice(-7).map(h => {
            const trimmed = h.substring(0, w - 2);
            return `║ ${pad(trimmed, w - 2)}║`;
        });

        // ── Milestone bar ──
        let msBar = '🏁 Sin milestones pendientes';
        if (nextMs) {
            const progress = Math.min(Math.round(followers / nextMs.target * 100), 99);
            const barLen = 20;
            const filled = Math.round(barLen * progress / 100);
            const bar = '█'.repeat(filled) + '░'.repeat(barLen - filled);
            msBar = `${nextMs.emoji || '🎯'} ${followers}/${nextMs.target} [${bar}] ${progress}%`;
        }

        const output = [
            '',
            `╔${line}╗`,
            `║   🎬 KICK CLIP BOT v2.1 — Smart Detect + Viral ${pad('', 15)}║`,
            `║   ${pad(`🇨🇴 Colombia (UTC-5) ${colTimeStr()}`, w - 4)}║`,
            `╠${line}╣`,
            `║  ⏱  Uptime: ${pad(this.formatUptime(), 15)} 💾 RAM: ${pad(this.memUsage(), 12)} 🔄 #${pad(this.stats.checks, 5)}║`,
            `╠${line}╣`,
            `║  📡 CANALES                                                      ║`,
            `║  ${thinLine.substring(0, w - 4)}  ║`,
            `║  Canal           Métricas           Estado                        ║`,
            `║  ${thinLine.substring(0, w - 4)}  ║`,
            ...channelLines,
            `╠${line}╣`,
            `║  📈 PIPELINE                                                     ║`,
            `║  ${thinLine.substring(0, w - 4)}  ║`,
            `║  Extraídos: ${pad(this.stats.clips_extracted, 5)} Procesados: ${pad(this.stats.clips_processed, 5)} Subidos: ${pad(this.stats.clips_uploaded, 10)}║`,
            `║  Fallidos : ${pad(this.stats.clips_failed, 5)} Virales   : ${pad(this.stats.viral_detections, 5)} Errores: ${pad(this.stats.errors, 10)}║`,
            `║  Hoy      : ${pad(`${this.stats.uploads_today}/${MAX_UPLOADS_PER_DAY} clips`, 15)} VOD clips: ${pad(this.stats.clips_from_vod, 19)}║`,
            `║  MinScore : ${pad(`${MIN_VIRAL_SCORE}/10`, 15)} Modos: ${pad(`🔴${Object.values(this.streamModes).filter(m=>m==='LIVE_MODE').length} 📼${Object.values(this.streamModes).filter(m=>m.startsWith('VOD')).length} ⚫${Object.values(this.streamModes).filter(m=>m==='SKIP'||m==='UNKNOWN').length}`, 22)}║`,
            `╠${line}╣`,
            `║  📋 COLA: ${pad(`⏳${pending} | ⚙️${procNow} | 📅${queued} | ✅${uploaded} | ❌${failed}`, w - 12)}║`,
            `║  📅 Próx. slot: ${pad(pub.next_slot, 30)} ${pub.can_publish ? '🟢 PUBLISH' : '🔴 ESPERAR'}     ║`,
            `╠${line}╣`,
            `║  💰 MONETIZACIÓN                                                 ║`,
            `║  ${thinLine.substring(0, w - 4)}  ║`,
            `║  👥 Followers: ${pad(followers.toLocaleString(), 12)} 👁 Views: ${pad(mon.total_views.toLocaleString(), 16)}║`,
            `║  ❤️  Likes   : ${pad(mon.total_likes.toLocaleString(), 12)} 📊 Eng  : ${pad(`${mon.avg_engagement}%`, 16)}║`,
            `║  🏆 Best: ${pad(`${bestClip.channel} — ${bestClip.title} (${bestClip.views.toLocaleString()}v)`, w - 12)}║`,
            `║  ${pad(msBar, w - 4)}  ║`,
            `╠${line}╣`,
            `║  📝 LOG                                                          ║`,
            `║  ${thinLine.substring(0, w - 4)}  ║`,
            ...histLines,
            `╠${line}╣`,
            `║  ${pad(this.getStatusLine(), w - 4)}  ║`,
            `╚${line}╝`,
            '',
        ];

        console.clear();
        console.log(output.join('\n'));
    }

    getStatusLine() {
        if (this.processing) {
            return '⚙️  Procesando pipeline viral...';
        }
        const pending = this.queue.filter(q => q.status === 'pending').length;
        const queued  = this.queue.filter(q => q.status === 'queued').length;
        if (pending > 0) {
            return `📋 ${pending} clips pendientes en pipeline`;
        }
        if (queued > 0) {
            return `📅 ${queued} clips esperando horario peak`;
        }
        if (this.stats.channels_live > 0) {
            return `👀 Monitoreando ${this.stats.channels_live} canales — buscando momentos virales`;
        }
        return '😴 Esperando canales en vivo... Ctrl+C para salir';
    }

    // ─── Loop Principal ──────────────────────────────────────────────────

    async processVodQueue() {
        /**
         * Procesa la cola de VODs en background.
         * Toma el siguiente VOD pendiente y extrae clips.
         */
        const item = this.vodQueue.find(v => v.status === 'pending');
        if (!item) return;

        if (!this.canUploadToday()) return;

        item.status = 'processing';
        const result = this.processVodChannel(item);
        item.status = result ? 'done' : 'failed';

        // Limpiar vodQueue
        this.vodQueue = this.vodQueue.filter(v => v.status === 'pending' || v.status === 'processing');
    }

    async start() {
        console.clear();
        console.log('\n╔══════════════════════════════════════════════════════════════════╗');
        console.log('║   🎬 KICK CLIP BOT v2.1 — Smart Stream Detector + Pipeline     ║');
        console.log('║   Canales: ' + pad(CHANNELS.join(', '), 53) + '║');
        console.log('║   Score mín: ' + pad(`${MIN_VIRAL_SCORE}/10`, 10) + ' Límite: ' + pad(`${MAX_UPLOADS_PER_DAY}/día`, 10) + ' TZ: COL (UTC-5)   ║');
        console.log('╚══════════════════════════════════════════════════════════════════╝\n');

        ensureDirs();
        logToFile('=== KickClipBot v2.1 INICIADO ===');

        // ═══ PASO 0: SMART STREAM DETECTOR (lo PRIMERO) ═══
        this.addHistory('🔍 Ejecutando Smart Stream Detector...');
        this.initialStartupScan();

        // Pausa para que el usuario vea la pantalla de inicio
        await sleep(3000);

        this.addHistory('🚀 Pipeline iniciado');

        // Dashboard loop
        setInterval(() => this.updateDashboard(), DASHBOARD_REFRESH);

        // Queue processing loop
        setInterval(() => this.processQueue(), 5000);

        // Queued items loop (check peak hours)
        setInterval(() => this.processQueuedItems(), 60000);

        // VOD queue loop (procesar VODs en background)
        setInterval(() => this.processVodQueue(), 30000);

        // Re-check offlines cada 15 min (detectar transiciones LIVE↔OFFLINE)
        setInterval(() => this.recheckOfflines(), RECHECK_INTERVAL);

        // Procesar VODs pendientes inmediatamente (una primera ronda)
        if (this.vodQueue.length > 0) {
            this.addHistory(`📼 ${this.vodQueue.length} canales VOD → extrayendo clips...`);
            await sleep(2000);
            await this.processVodQueue();
        }

        // Loop principal
        while (true) {
            try {
                // 1. Chequear canales LIVE (solo los que están en modo LIVE_MODE)
                const liveChannels = Object.keys(this.streamModes).filter(
                    ch => this.streamModes[ch] === 'LIVE_MODE'
                );

                // También re-scan general periódico para actualizar viewers/status
                if (this.stats.checks % 5 === 0) {
                    // Cada 5 checks, re-escanear todos con monitor
                    const monitored = this.monitorChannels();
                    // Actualizar lista de lives
                    for (const ch of monitored) {
                        if (this.streamModes[ch] !== 'LIVE_MODE') {
                            this.streamModes[ch] = 'LIVE_MODE';
                            this.streamPriorities[ch] = 1;
                            if (this.channels[ch]) this.channels[ch].mode = 'LIVE_MODE';
                        }
                    }
                }
                this.stats.checks++;

                // 2. Para cada canal en vivo → detección viral
                for (const channel of liveChannels) {
                    if (!this.channels[channel]) continue;
                    if (!this.canClipChannel(channel)) continue;
                    if (!this.canUploadToday()) {
                        this.addHistory('⏳ Límite diario alcanzado');
                        break;
                    }

                    // Detección viral (live analysis via stream_detector + viral_detector)
                    let viralScore = 0;

                    try {
                        // Primero: snapshot rápido via stream_detector LiveAnalyzer
                        const snapOutput = runPyScript('stream_detector.py', ['--live-analyze', channel, '30'], 45000);
                        if (snapOutput) {
                            const snapData = JSON.parse(snapOutput);
                            const peaks = snapData.peaks || [];
                            viralScore = snapData.max_viral_score || 0;

                            if (peaks.length > 0) {
                                this.addHistory(`🔥 ${channel}: ${peaks.length} pico(s) detectado(s)!`);
                            }
                        }
                    } catch (e) {
                        // Fallback a viral_detector.py
                        const viralData = this.detectViral(channel);
                        viralScore = viralData ? (viralData.viral_score || 0) : 0;
                    }

                    if (this.channels[channel]) {
                        this.channels[channel].viral_score = viralScore;
                    }

                    // Decidir si clipear
                    let shouldClip = false;

                    if (viralScore >= MIN_VIRAL_SCORE) {
                        shouldClip = true;
                        this.addHistory(`🔥 ${channel} viral=${viralScore.toFixed(1)} → CLIPEAR`);
                    } else if (viralScore >= MIN_VIRAL_SCORE - 2) {
                        shouldClip = Math.random() < 0.3;
                        if (shouldClip) {
                            this.addHistory(`⚡ ${channel} score=${viralScore.toFixed(1)} → probando`);
                        }
                    }

                    if (shouldClip) {
                        const title = this.channels[channel] ? this.channels[channel].title : '';
                        this.addToQueue(channel, title, viralScore);
                    }

                    await sleep(rand(3000, 8000));
                }

            } catch (e) {
                this.stats.errors++;
                this.addHistory(`❌ Loop: ${e.message.split('\n')[0].substring(0, 45)}`);
                logToFile(`ERROR loop: ${e.message}`);
            }

            // Esperar antes del próximo check
            await sleep(CHECK_INTERVAL);
        }
    }
}

// ─── Main ────────────────────────────────────────────────────────────────────

const bot = new KickClipBot();

process.on('SIGINT', () => {
    console.log('\n\n🛑 Deteniendo KickClipBot v2.1...');
    logToFile('=== KickClipBot v2.1 DETENIDO (SIGINT) ===');
    process.exit(0);
});

process.on('uncaughtException', (e) => {
    console.error('[UNCAUGHT]', e.message);
    logToFile(`UNCAUGHT: ${e.message}`);
});

process.on('unhandledRejection', (e) => {
    console.error('[UNHANDLED]', e);
    logToFile(`UNHANDLED: ${e}`);
});

bot.start().catch(e => {
    console.error('Error fatal:', e);
    logToFile(`FATAL: ${e.message}`);
    process.exit(1);
});

setInterval(() => {}, 1000);
