# 🎬 KickClipBot v1.0

Automatiza la extracción de clips de streams en **Kick.com** y los sube automáticamente a **TikTok** en formato vertical (9:16).

## 📁 Estructura del Proyecto

```
TIKTOK/
├── kick_clip_bot.js      # 🎯 Orquestador principal (Node.js)
├── clip_monitor.py       # 👁️ Monitor de streams en vivo
├── clip_extractor.py     # 📥 Extractor de clips (API + HLS)
├── video_processor.py    # 🎬 Procesador de video (9:16 + watermark + subs)
├── tiktok_uploader.py    # 📤 Uploader automático a TikTok
├── config.json           # ⚙️ Configuración general
├── .env.example          # 🔑 Template de variables de entorno
├── .env                  # 🔑 Variables de entorno (TUS credenciales)
├── kick-websocket.js     # Bot de viewers (referencia)
├── clips/                # Clips descargados (raw)
├── processed/            # Videos procesados para TikTok
├── uploaded/             # Videos ya subidos
├── logs/                 # Logs del bot
├── temp/                 # Archivos temporales
└── cookies/              # Sesiones persistentes de TikTok
```

## 🚀 Instalación

### 1. Requisitos del Sistema

```bash
# macOS
brew install ffmpeg
brew install python3 node

# Linux (Ubuntu/Debian)
sudo apt update && sudo apt install -y ffmpeg python3 python3-pip nodejs npm
```

### 2. Dependencias de Python

```bash
pip install curl_cffi streamlink ffmpeg-python openai-whisper pillow requests
pip install playwright
python -m playwright install chromium
```

### 3. Dependencias de Node.js

```bash
cd /Users/jhon/Documents/TIKTOK
npm install ws axios
```

### 4. Configurar Variables de Entorno

```bash
# Copiar el template
cp .env.example .env

# Editar con tus credenciales
nano .env
```

Rellena al menos:
- `TIKTOK_EMAIL` — Tu email de TikTok
- `TIKTOK_PASSWORD` — Tu contraseña de TikTok

## ⚙️ Configuración

Edita `config.json` para personalizar:

| Sección | Descripción |
|---------|-------------|
| `channels` | Lista de canales de Kick a monitorear |
| `clip_settings` | Duración de clips, rate limiting, filtros |
| `video_settings` | Resolución, watermark, subtítulos, bitrate |
| `tiktok_settings` | Delays de upload, hashtags, límites diarios |
| `monitor_settings` | Intervalo de chequeo, umbrales de engagement |

### Canales por defecto:
- lonche
- willito
- westcol
- lasapaaaaa
- chanty
- Juanferlamata

## 🎮 Uso

### Ejecutar el Bot Completo (recomendado)

```bash
node kick_clip_bot.js
```

Esto inicia el dashboard interactivo que:
1. Monitorea los 6 canales simultáneamente
2. Detecta streams en vivo y engagement alto
3. Extrae clips automáticamente
4. Los procesa a formato TikTok (1080x1920)
5. Los sube a TikTok con títulos y hashtags generados

### Usar Módulos Individualmente

```bash
# Chequear todos los canales
python3 clip_monitor.py

# Chequear un canal específico
python3 clip_monitor.py lonche

# Solo canales en vivo
python3 clip_monitor.py --live-only

# Chequear engagement
python3 clip_monitor.py --engagement westcol

# Obtener clips de un canal (via API)
python3 clip_extractor.py lonche --api

# Capturar 30s del stream en vivo
python3 clip_extractor.py westcol --capture 30

# Obtener el mejor clip disponible
python3 clip_extractor.py chanty --best

# Procesar video a formato TikTok
python3 video_processor.py ./clips/clip.mp4 lonche

# Procesar sin subtítulos
python3 video_processor.py ./clips/clip.mp4 lonche --no-subs

# Subir a TikTok
python3 tiktok_uploader.py ./processed/video.mp4 lonche "Título del stream"
```

## 📊 Dashboard

El bot muestra un dashboard en tiempo real actualizado cada 10 segundos:

```
╔══════════════════════════════════════════════════════════════╗
║   🎬 KICK CLIP BOT v1.0 — Auto Clips → TikTok             ║
╠══════════════════════════════════════════════════════════════╣
║  📊 CANALES                                                 ║
║  🔴 lonche          1.2k v ⚡75   📎2    ✅ Subido          ║
║  🔴 westcol         5.8k v ⚡92   📎3    🎬 Procesando...   ║
║  ⚫ willito                               ⚫ Offline         ║
║  ...                                                        ║
╠══════════════════════════════════════════════════════════════╣
║  📈 Clips: 5 extraídos | 4 procesados | 3 subidos          ║
║  📋 Cola: 1 pendiente | 1 procesando                       ║
╚══════════════════════════════════════════════════════════════╝
```

## 🔒 Rate Limiting

- **Máximo 5 clips por hora** por canal (configurable)
- **Máximo 20 uploads por día** a TikTok (configurable)
- **Delay aleatorio** entre uploads (2-10 minutos) para evitar detección de spam
- **Engagement scoring** para priorizar clips de momentos virales

## 🔧 Solución de Problemas

| Problema | Solución |
|----------|----------|
| `curl_cffi` error | `pip install curl_cffi --upgrade` |
| `streamlink` no captura | Verificar que el canal está en vivo |
| `ffmpeg` no encontrado | `brew install ffmpeg` (macOS) |
| TikTok login falla | Puede requerir CAPTCHA manual la primera vez |
| Whisper muy lento | Usa `--no-subs` o cambia `whisper_model` a `"tiny"` en config |
| Rate limited por Kick | Aumentar `check_interval_sec` en config |

## 📋 Pipeline de Procesamiento

```
Canal en vivo detectado
    │
    ▼
Análisis de engagement (viewers + chat)
    │
    ▼ (engagement > umbral)
Extracción de clip (API → Streamlink fallback)
    │
    ▼
Procesamiento FFmpeg (9:16 + watermark + subs)
    │
    ▼
Delay aleatorio (anti-spam)
    │
    ▼
Upload a TikTok (Playwright → API fallback)
    │
    ▼
Movido a ./uploaded/ ✅
```

## ⚠️ Notas Importantes

- **Primera ejecución de TikTok**: Es probable que requiera resolver un CAPTCHA manualmente. Ejecuta primero con `headless_browser: false` en config.json, resuelve el CAPTCHA, y luego cambia a `true`.
- **Whisper**: El modelo `base` ocupa ~1GB de RAM. Usa `tiny` para equipos con poca memoria.
- **Cookies**: Las sesiones de TikTok se guardan en `./cookies/`. No borres esta carpeta si quieres mantener la sesión.
- **Logs**: Revisa `./logs/` para diagnóstico detallado de errores.

## 📄 Licencia

Uso personal y educativo. No nos hacemos responsables del uso de esta herramienta.
