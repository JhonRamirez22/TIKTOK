#!/usr/bin/env node
/**
 * KickClipBot v1.0 — Orquestador Principal
 * ═══════════════════════════════════════════════════════════════
 * Monitorea canales de Kick, extrae clips, los procesa para
 * formato TikTok (9:16) y los sube automáticamente.
 *
 * USO: node kick_clip_bot.js
 * REQ: pip install curl_cffi streamlink ffmpeg-python openai-whisper playwright
 *      npm install ws axios
 *      brew install ffmpeg  (macOS) / apt install ffmpeg (Linux)
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
const DASHBOARD_REFRESH = (CONFIG.dashboard || {}).refresh_interval_ms || 10000;

const MAX_CLIPS_PER_HOUR = CLIP_CFG.max_clips_per_hour_per_channel || 5;
const CHECK_INTERVAL     = (MONITOR_CFG.check_interval_sec || 30) * 1000;
const MAX_UPLOADS_PER_DAY = TIKTOK_CFG.max_uploads_per_day || 20;

// ─── Utilidades ──────────────────────────────────────────────────────────────

const PID_PREFIX = `_kcb_${process.pid}_`;

function pad(s, n)  { return String(s).padEnd(n); }
function padL(s, n) { return String(s).padStart(n); }

function rand(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

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

// ─── Clase Principal ─────────────────────────────────────────────────────────

class KickClipBot {
    constructor() {
        this.startTime = Date.now();
        this.channels  = {};
        this.queue     = [];           // Cola de procesamiento
        this.processing = false;       // Lock de procesamiento
        this.history   = [];           // Historial de acciones recientes

        // Contadores globales
        this.stats = {
            checks:          0,
            channels_live:   0,
            clips_found:     0,
            clips_extracted: 0,
            clips_processed: 0,
            clips_uploaded:  0,
            clips_failed:    0,
            errors:          0,
            uploads_today:   0,
            uploads_today_date: new Date().toISOString().split('T')[0],
        };

        // Rate limiting por canal: { canal: { clips_this_hour: N, hour_start: timestamp } }
        this.rateLimit = {};

        // Inicializar estado de canales
        for (const ch of CHANNELS) {
            this.channels[ch] = {
                name:          ch,
                is_live:       false,
                viewers:       0,
                title:         '',
                category:      '',
                engagement:    0,
                last_check:    null,
                last_clip:     null,
                clips_today:   0,
                status:        '⏳ Pendiente',
            };
            this.rateLimit[ch] = { clips_this_hour: 0, hour_start: Date.now() };
        }
    }

    // ─── Formateo ────────────────────────────────────────────────────────

    formatUptime() {
        const s = Math.floor((Date.now() - this.startTime) / 1000);
        const h = String(Math.floor(s / 3600)).padStart(2, '0');
        const m = String(Math.floor((s % 3600) / 60)).padStart(2, '0');
        const sec = String(s % 60).padStart(2, '0');
        return `${h}:${m}:${sec}`;
    }

    memUsage() {
        return `${(process.memoryUsage().rss / 1024 / 1024).toFixed(1)} MB`;
    }

    timestamp() {
        return new Date().toLocaleTimeString('es-ES', { hour12: false });
    }

    // ─── Rate Limiting ───────────────────────────────────────────────────

    canClipChannel(channel) {
        const rl = this.rateLimit[channel];
        if (!rl) return true;

        const now = Date.now();
        const hourMs = 60 * 60 * 1000;

        // Reset si pasó la hora
        if (now - rl.hour_start >= hourMs) {
            rl.clips_this_hour = 0;
            rl.hour_start = now;
        }

        return rl.clips_this_hour < MAX_CLIPS_PER_HOUR;
    }

    recordClip(channel) {
        if (!this.rateLimit[channel]) {
            this.rateLimit[channel] = { clips_this_hour: 0, hour_start: Date.now() };
        }
        this.rateLimit[channel].clips_this_hour++;
    }

    canUploadToday() {
        const today = new Date().toISOString().split('T')[0];
        if (this.stats.uploads_today_date !== today) {
            this.stats.uploads_today = 0;
            this.stats.uploads_today_date = today;
        }
        return this.stats.uploads_today < MAX_UPLOADS_PER_DAY;
    }

    // ─── Paso 1: Monitorear Canales ──────────────────────────────────────

    monitorChannels() {
        this.addHistory('🔍 Chequeando canales...');

        try {
            const output = runPyScript('clip_monitor.py', [], 60000);
            if (!output) {
                this.addHistory('⚠️ Monitor no retornó datos');
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
                this.channels[ch].title      = (info.title || '').substring(0, 50);
                this.channels[ch].category   = (info.category || '').substring(0, 20);
                this.channels[ch].last_check = this.timestamp();

                if (info.is_live) {
                    this.channels[ch].status = `🔴 LIVE (${info.viewers} viewers)`;
                    liveChannels.push(ch);
                } else {
                    this.channels[ch].status = '⚫ Offline';
                }

                if (info.error) {
                    this.channels[ch].status = `⚠️ Error: ${String(info.error).substring(0, 30)}`;
                }
            }

            this.stats.channels_live = liveChannels.length;
            this.stats.checks++;

            if (liveChannels.length > 0) {
                this.addHistory(`✅ ${liveChannels.length} canales en vivo: ${liveChannels.join(', ')}`);
            } else {
                this.addHistory('⚫ Ningún canal en vivo');
            }

            return liveChannels;

        } catch (e) {
            this.stats.errors++;
            this.addHistory(`❌ Error monitor: ${e.message.split('\n')[0].substring(0, 60)}`);
            logToFile(`ERROR monitor: ${e.message}`);
            return [];
        }
    }

    // ─── Paso 2: Chequear Engagement ─────────────────────────────────────

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
            this.addHistory(`⏳ Rate limit: ${channel} (${this.rateLimit[channel].clips_this_hour}/${MAX_CLIPS_PER_HOUR} clips/hora)`);
            return null;
        }

        this.addHistory(`📥 Extrayendo clip de ${channel}...`);
        if (this.channels[channel]) {
            this.channels[channel].status = '📥 Extrayendo clip...';
        }

        try {
            const output = runPyScript('clip_extractor.py', [channel, '--best'], 180000);
            if (!output) {
                this.addHistory(`⚠️ Extractor no retornó datos para ${channel}`);
                this.stats.errors++;
                return null;
            }

            const result = JSON.parse(output);

            if (result.error) {
                this.addHistory(`⚠️ ${channel}: ${String(result.error).substring(0, 60)}`);
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
                this.addHistory(`✅ Clip extraído: ${result.filename || path.basename(result.path)} (${result.size_mb || '?'} MB)`);
                logToFile(`CLIP EXTRAÍDO: ${channel} → ${result.path}`);
                return result;
            }

            return null;

        } catch (e) {
            this.stats.errors++;
            this.stats.clips_failed++;
            this.addHistory(`❌ Error extrayendo ${channel}: ${e.message.split('\n')[0].substring(0, 60)}`);
            logToFile(`ERROR extractor ${channel}: ${e.message}`);
            return null;
        }
    }

    // ─── Paso 4: Procesar Video ──────────────────────────────────────────

    processVideo(clipResult, channel) {
        if (!clipResult || !clipResult.path) return null;

        this.addHistory(`🎬 Procesando video de ${channel}...`);
        if (this.channels[channel]) {
            this.channels[channel].status = '🎬 Procesando video...';
        }

        try {
            const args = [clipResult.path, channel];

            // Desactivar subtítulos si Whisper no está instalado
            // (el script lo maneja internamente, pero podemos forzar --no-subs para velocidad)
            // args.push('--no-subs');  // Descommentar si Whisper es muy lento

            const output = runPyScript('video_processor.py', args, 300000);
            if (!output) {
                this.addHistory(`⚠️ Procesador no retornó datos para ${channel}`);
                this.stats.errors++;
                return null;
            }

            const result = JSON.parse(output);

            if (result.error) {
                this.addHistory(`⚠️ Proceso ${channel}: ${String(result.error).substring(0, 60)}`);
                this.stats.clips_failed++;
                return null;
            }

            if (result.path) {
                this.stats.clips_processed++;
                this.addHistory(`✅ Video procesado: ${result.filename} (${result.size_mb} MB, ${Math.round(result.duration || 0)}s)`);
                logToFile(`VIDEO PROCESADO: ${channel} → ${result.path}`);
                return result;
            }

            return null;

        } catch (e) {
            this.stats.errors++;
            this.stats.clips_failed++;
            this.addHistory(`❌ Error procesando ${channel}: ${e.message.split('\n')[0].substring(0, 60)}`);
            logToFile(`ERROR procesador ${channel}: ${e.message}`);
            return null;
        }
    }

    // ─── Paso 5: Subir a TikTok ─────────────────────────────────────────

    uploadToTiktok(processedResult, channel, streamTitle = '') {
        if (!processedResult || !processedResult.path) return null;

        if (!this.canUploadToday()) {
            this.addHistory(`⏳ Límite diario de uploads alcanzado (${this.stats.uploads_today}/${MAX_UPLOADS_PER_DAY})`);
            return null;
        }

        this.addHistory(`📤 Subiendo a TikTok: ${channel}...`);
        if (this.channels[channel]) {
            this.channels[channel].status = '📤 Subiendo a TikTok...';
        }

        try {
            const args = [processedResult.path, channel];
            if (streamTitle) args.push(streamTitle);

            const output = runPyScript('tiktok_uploader.py', args, 300000);
            if (!output) {
                this.addHistory(`⚠️ Uploader no retornó datos para ${channel}`);
                this.stats.errors++;
                return null;
            }

            const result = JSON.parse(output);

            if (result.success) {
                this.stats.clips_uploaded++;
                this.stats.uploads_today++;
                this.addHistory(`🎉 ¡SUBIDO! ${channel} → TikTok (${result.method})`);
                logToFile(`UPLOAD OK: ${channel} → TikTok via ${result.method}`);
                if (this.channels[channel]) {
                    this.channels[channel].status = `✅ Subido a TikTok (${this.timestamp()})`;
                }
                return result;
            }

            if (result.needs_manual_login) {
                this.addHistory(`🔑 TikTok requiere login manual (CAPTCHA). Ver screenshot.`);
            } else {
                this.addHistory(`⚠️ Upload falló: ${String(result.error || '?').substring(0, 60)}`);
            }
            this.stats.clips_failed++;
            return result;

        } catch (e) {
            this.stats.errors++;
            this.stats.clips_failed++;
            this.addHistory(`❌ Error upload ${channel}: ${e.message.split('\n')[0].substring(0, 60)}`);
            logToFile(`ERROR upload ${channel}: ${e.message}`);
            return null;
        }
    }

    // ─── Cola de Procesamiento ───────────────────────────────────────────

    addToQueue(channel, streamTitle = '') {
        // Evitar duplicados en cola
        if (this.queue.find(q => q.channel === channel && q.status === 'pending')) {
            return;
        }
        this.queue.push({
            channel,
            streamTitle,
            status: 'pending',
            addedAt: Date.now(),
        });
        this.stats.clips_found++;
    }

    async processQueue() {
        if (this.processing) return;
        if (this.queue.length === 0) return;

        this.processing = true;

        // Tomar el siguiente item pendiente
        const item = this.queue.find(q => q.status === 'pending');
        if (!item) {
            this.processing = false;
            return;
        }

        item.status = 'processing';

        try {
            // Paso 1: Extraer clip
            const clipResult = this.extractClip(item.channel);
            if (!clipResult) {
                item.status = 'failed';
                this.processing = false;
                return;
            }

            // Delay entre pasos
            await sleep(rand(2000, 5000));

            // Paso 2: Procesar video
            const processedResult = this.processVideo(clipResult, item.channel);
            if (!processedResult) {
                item.status = 'failed';
                this.processing = false;
                return;
            }

            // Delay antes de upload
            const uploadDelay = rand(
                (TIKTOK_CFG.upload_delay_min_sec || 120) * 1000,
                (TIKTOK_CFG.upload_delay_max_sec || 600) * 1000
            );
            this.addHistory(`⏳ Esperando ${Math.round(uploadDelay / 1000)}s antes de upload...`);
            await sleep(uploadDelay);

            // Paso 3: Subir a TikTok
            const uploadResult = this.uploadToTiktok(processedResult, item.channel, item.streamTitle);

            item.status = uploadResult && uploadResult.success ? 'uploaded' : 'failed';

        } catch (e) {
            item.status = 'failed';
            this.stats.errors++;
            this.addHistory(`❌ Error en pipeline: ${e.message.split('\n')[0].substring(0, 60)}`);
        }

        // Limpiar items completados antiguos (mantener últimos 20)
        this.queue = this.queue.filter(q => q.status === 'pending' || q.status === 'processing')
            .concat(this.queue.filter(q => q.status !== 'pending' && q.status !== 'processing').slice(-20));

        this.processing = false;
    }

    // ─── Historial ───────────────────────────────────────────────────────

    addHistory(msg) {
        const entry = `[${this.timestamp()}] ${msg}`;
        this.history.push(entry);
        if (this.history.length > 15) this.history.shift();
    }

    // ─── Dashboard ───────────────────────────────────────────────────────

    updateDashboard() {
        const today = new Date().toISOString().split('T')[0];
        if (this.stats.uploads_today_date !== today) {
            this.stats.uploads_today = 0;
            this.stats.uploads_today_date = today;
        }

        const w = 62; // Ancho interior
        const line = '═'.repeat(w);
        const thinLine = '─'.repeat(w);

        // Construir canales
        let channelLines = [];
        for (const ch of CHANNELS) {
            const c = this.channels[ch] || {};
            const live = c.is_live ? '🔴' : '⚫';
            const viewers = c.is_live ? `${c.viewers}v` : '';
            const eng = c.engagement > 0 ? `⚡${c.engagement}` : '';
            const clips = c.clips_today > 0 ? `📎${c.clips_today}` : '';
            const status = (c.status || '').substring(0, 28);
            const info = [viewers, eng, clips].filter(Boolean).join(' ');

            channelLines.push(
                `║ ${live} ${pad(ch, 15)} ${pad(info, 16)} ${pad(status, 26)}║`
            );
        }

        // Cola
        const pending  = this.queue.filter(q => q.status === 'pending').length;
        const procNow  = this.queue.filter(q => q.status === 'processing').length;
        const uploaded = this.queue.filter(q => q.status === 'uploaded').length;
        const failed   = this.queue.filter(q => q.status === 'failed').length;

        // Historial (últimas 8 líneas)
        const histLines = this.history.slice(-8).map(h => {
            const trimmed = h.substring(0, w - 2);
            return `║ ${pad(trimmed, w - 2)}║`;
        });

        const output = [
            '',
            `╔${line}╗`,
            `║   🎬 KICK CLIP BOT v1.0 — Auto Clips → TikTok          ║`,
            `╠${line}╣`,
            `║  Uptime     : ${pad(this.formatUptime(), w - 17)}║`,
            `║  RAM        : ${pad(this.memUsage(), w - 17)}║`,
            `║  Checks     : ${pad(this.stats.checks, w - 17)}║`,
            `╠${line}╣`,
            `║  📊 CANALES                                              ║`,
            `╠${line}╣`,
            `║  Canal           Info             Estado                  ║`,
            `║  ${thinLine.substring(0, w - 4)}  ║`,
            ...channelLines,
            `╠${line}╣`,
            `║  📈 ESTADÍSTICAS                                         ║`,
            `╠${line}╣`,
            `║  Clips extraídos  : ${pad(this.stats.clips_extracted, 10)} Clips procesados : ${pad(this.stats.clips_processed, 10)}║`,
            `║  Subidos a TikTok : ${pad(this.stats.clips_uploaded, 10)} Fallidos         : ${pad(this.stats.clips_failed, 10)}║`,
            `║  Uploads hoy      : ${pad(`${this.stats.uploads_today}/${MAX_UPLOADS_PER_DAY}`, 10)} Errores totales  : ${pad(this.stats.errors, 10)}║`,
            `╠${line}╣`,
            `║  📋 COLA: ${pad(`${pending} pendientes | ${procNow} procesando | ${uploaded} subidos | ${failed} fallidos`, w - 12)}║`,
            `╠${line}╣`,
            `║  📝 LOG RECIENTE                                         ║`,
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
            return '⚙️  Procesando clip...';
        }
        const pending = this.queue.filter(q => q.status === 'pending').length;
        if (pending > 0) {
            return `📋 ${pending} clips en cola esperando procesamiento`;
        }
        if (this.stats.channels_live > 0) {
            return `👀 Monitoreando ${this.stats.channels_live} canales en vivo`;
        }
        return '😴 Esperando que algún canal se ponga en vivo...';
    }

    // ─── Loop Principal ──────────────────────────────────────────────────

    async start() {
        console.clear();
        console.log('\n╔══════════════════════════════════════════════════════════════╗');
        console.log('║   🎬 KICK CLIP BOT v1.0 — Iniciando...                     ║');
        console.log('║   Canales: ' + pad(CHANNELS.join(', '), 49) + '║');
        console.log('╚══════════════════════════════════════════════════════════════╝\n');

        ensureDirs();
        this.addHistory('🚀 KickClipBot iniciado');
        logToFile('=== KickClipBot INICIADO ===');

        // Dashboard loop
        setInterval(() => this.updateDashboard(), DASHBOARD_REFRESH);

        // Queue processing loop
        setInterval(() => this.processQueue(), 5000);

        // Loop principal de monitoreo
        while (true) {
            try {
                // 1. Chequear todos los canales
                const liveChannels = this.monitorChannels();

                // 2. Para cada canal en vivo, evaluar si extraer clip
                for (const channel of liveChannels) {
                    // Rate limiting
                    if (!this.canClipChannel(channel)) continue;
                    if (!this.canUploadToday()) {
                        this.addHistory('⏳ Límite diario alcanzado, esperando mañana...');
                        break;
                    }

                    // Chequear engagement
                    const engagement = this.checkEngagement(channel);
                    const isHighEngagement = engagement && engagement.is_high_engagement;

                    // Decidir si clipear
                    const chData = this.channels[channel];
                    const engScore = chData ? chData.engagement : 0;

                    // Siempre clipear si hay engagement alto, o probabilidad basada en score
                    const shouldClip = isHighEngagement ||
                        engScore >= 40 ||
                        Math.random() < 0.3; // 30% chance para cualquier canal vivo

                    if (shouldClip) {
                        const streamTitle = chData ? chData.title : '';
                        this.addToQueue(channel, streamTitle);
                        this.addHistory(`📎 ${channel} agregado a cola (engagement: ${engScore})`);
                    }

                    // Delay entre canales
                    await sleep(rand(2000, 5000));
                }

            } catch (e) {
                this.stats.errors++;
                this.addHistory(`❌ Error en loop: ${e.message.split('\n')[0].substring(0, 50)}`);
                logToFile(`ERROR loop principal: ${e.message}`);
            }

            // Esperar antes del próximo check
            await sleep(CHECK_INTERVAL);
        }
    }
}

// ─── Utilidades ──────────────────────────────────────────────────────────────

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── Main ────────────────────────────────────────────────────────────────────

const bot = new KickClipBot();

process.on('SIGINT', () => {
    console.log('\n\n🛑 Deteniendo KickClipBot...');
    logToFile('=== KickClipBot DETENIDO (SIGINT) ===');
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

// Keepalive
setInterval(() => {}, 1000);
