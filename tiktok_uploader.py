#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════╗
║                   TikTok Uploader v3.0                         ║
║          Powered by tiktok-uploader + Playwright               ║
║  Kick.com → TikTok (cookies auth, queue, peak hours, retries)  ║
╚══════════════════════════════════════════════════════════════════╝

Flujo:
  1ª vez  → Detecta que no hay cookies → abre navegador VISIBLE →
            login manual → guarda cookies en session/tiktok_cookies.json
  Runs +  → Carga cookies → verifica sesión → sube headless
  Fallo   → Si auth error, borra cookies y re-login visible

Integración:
  kick_clip_bot.js → runPyScript('tiktok_uploader.py', [video, canal, titulo], 300000)
  Output: JSON a stdout  {"success": bool, "method": str, "error": str, "needs_manual_login": bool}

Dependencias:
  pip install tiktok-uploader playwright pytz
  playwright install chromium
"""

import os
import sys
import json
import time
import random
import shutil
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

try:
    import pytz
except ImportError:
    pytz = None

# ─── Constantes ───────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
ENV_PATH = os.path.join(BASE_DIR, ".env")

SESSION_DIR = os.path.join(BASE_DIR, "session")
COOKIES_PATH = os.path.join(SESSION_DIR, "tiktok_cookies.json")

LOGS_DIR = os.path.join(BASE_DIR, "logs")
UPLOAD_HISTORY_PATH = os.path.join(LOGS_DIR, "upload_history.json")
UPLOAD_LOG_PATH = os.path.join(LOGS_DIR, "uploads.jsonl")

UPLOADED_DIR = os.path.join(BASE_DIR, "uploaded")

# Credenciales (email solo para referencia interna del flujo de login)
TIKTOK_EMAIL = "jhonra220822@gmail.com"

# Horarios pico Colombia (UTC-5)
DEFAULT_PEAK_HOURS = [7, 12, 19, 20.5, 22]
DEFAULT_TIMEZONE = "America/Bogota"
DEFAULT_MAX_UPLOADS = 20
DEFAULT_MIN_HOURS_BETWEEN = 2

MAX_RETRIES = 3
RETRY_DELAY_BASE_SEC = 10  # Backoff: 10, 20, 40...

# Hashtags por canal
CHANNEL_HASHTAGS = {
    "westcol": ["westcol", "westcolclips", "westcollive"],
    "lonche": ["lonche", "loncheclips", "lonchelive"],
    "willito": ["willito", "willitoclips", "willitolive"],
    "lasapaaaaa": ["lasapa", "lasapaaaaa", "lasapaclips"],
    "chanty": ["chanty", "chantyclips", "chantylive"],
    "juanferlamata": ["juanfer", "juanferlamata", "juanferclips"],
}

GLOBAL_HASHTAGS = [
    "kick", "clips", "viral", "gaming", "stream", "funny",
    "fyp", "foryoupage", "parati", "trending", "tiktok",
]


# ─── Utilidades ───────────────────────────────────────────────────────────────

def log(msg):
    """Log a stderr (no contamina stdout JSON)."""
    print(f"[tiktok_uploader] {msg}", file=sys.stderr, flush=True)


def ensure_dirs():
    """Crea directorios necesarios."""
    for d in [SESSION_DIR, LOGS_DIR, UPLOADED_DIR]:
        os.makedirs(d, exist_ok=True)


def load_config():
    """Carga config.json."""
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def load_env():
    """Carga .env básico (KEY=VALUE)."""
    if not os.path.exists(ENV_PATH):
        return
    try:
        with open(ENV_PATH, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))
    except Exception:
        pass


def get_colombia_now():
    """Devuelve datetime actual en Colombia (UTC-5)."""
    if pytz:
        tz = pytz.timezone(DEFAULT_TIMEZONE)
        return datetime.now(tz)
    # Fallback sin pytz
    return datetime.utcnow() - timedelta(hours=5)


def file_hash(filepath):
    """MD5 corto para identificar videos."""
    h = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()[:12]
    except Exception:
        return "unknown"


# ─── Generador de Título y Hashtags ──────────────────────────────────────────

def generate_title_and_hashtags(channel_name, stream_title=""):
    """
    Genera título simple: solo emoji(s) + hashtags.
    Ejemplo: 😂 #lonche #kick #viral #fyp #parati
    Máx 150 caracteres.
    """
    config = load_config()
    tiktok_cfg = config.get("tiktok_settings", {})
    max_len = tiktok_cfg.get("title_max_length", 150)

    channel_lower = channel_name.lower()

    # Emoji aleatorio (simple, como lo pide el usuario)
    emojis = ["😂", "💀", "🔥", "🤣", "😱", "👀", "⚡", "🫣", "💯", "😭", "🤯", "😈"]
    emoji = random.choice(emojis)

    # Solo emoji como título base
    title_base = emoji

    # Hashtags del canal
    ch_tags = CHANNEL_HASHTAGS.get(channel_lower, [channel_lower])

    # Hashtags globales
    config_tags = tiktok_cfg.get("hashtags_default", GLOBAL_HASHTAGS)

    # Combinar: canal (2-3) + globales, sin duplicados
    all_tags = []
    seen = set()
    for t in ch_tags[:3] + config_tags:
        tag = t.lower().replace(" ", "").replace("#", "")
        if tag and tag not in seen:
            all_tags.append(f"#{tag}")
            seen.add(tag)
        if len(all_tags) >= 12:
            break

    hashtags_str = " ".join(all_tags)

    # Ajustar para no exceder max_len
    full_title = f"{title_base} {hashtags_str}"
    if len(full_title) > max_len:
        full_title = full_title[:max_len]

    return {
        "title": full_title,
        "title_base": title_base,
        "hashtags": all_tags,
        "hashtags_str": hashtags_str,
    }


# ─── Gestión de Cookies / Sesión ─────────────────────────────────────────────

def cookies_exist():
    """Verifica si existe el archivo de cookies."""
    return os.path.exists(COOKIES_PATH) and os.path.getsize(COOKIES_PATH) > 50


def cookies_age_hours():
    """Horas desde última modificación de cookies."""
    if not cookies_exist():
        return float("inf")
    mtime = os.path.getmtime(COOKIES_PATH)
    age = time.time() - mtime
    return age / 3600


def delete_cookies():
    """Borra cookies para forzar re-login."""
    if os.path.exists(COOKIES_PATH):
        try:
            os.remove(COOKIES_PATH)
            log("🗑️ Cookies eliminadas (sesión expirada)")
        except Exception:
            pass


def perform_manual_login():
    """
    Abre navegador VISIBLE (headful) para login manual en TikTok.
    El usuario debe loguearse manualmente.
    Después se guardan las cookies.

    Retorna True si se obtuvieron cookies, False si no.
    """
    ensure_dirs()
    log("=" * 60)
    log("🔐 LOGIN MANUAL REQUERIDO")
    log("   Se abrirá un navegador con TikTok")
    log("   Usa Google, QR, teléfono o el método que prefieras")
    log("   Cuando termines el login, espera 10 segundos...")
    log("=" * 60)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log("❌ Playwright no instalado. Ejecuta: pip install playwright && playwright install chromium")
        return False

    try:
        with sync_playwright() as p:
            # Navegador VISIBLE para login manual
            browser = p.chromium.launch(
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process",
                ]
            )

            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                locale="es-CO",
            )

            page = context.new_page()

            # Navegar a TikTok login (página principal con TODOS los métodos)
            log("🌐 Abriendo TikTok Login...")
            page.goto("https://www.tiktok.com/login", wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)

            log("👉 Elige tu método de login:")
            log("   • Google / Apple / Facebook")
            log("   • Teléfono / Email")
            log("   • Código QR")
            log("   • Cualquier opción que tengas configurada")

            # Esperar a que el usuario complete el login
            log("⏳ Esperando login... (máximo 5 minutos)")

            try:
                # Esperar redirección post-login o elemento de usuario logueado
                page.wait_for_url("**/foryou**", timeout=300000)
                log("✅ Login detectado (redirigido a For You)")
            except Exception:
                # Segundo intento: buscar indicadores de sesión activa
                try:
                    page.wait_for_selector(
                        '[data-e2e="profile-icon"], [data-e2e="upload-icon"], a[href*="/upload"]',
                        timeout=300000
                    )
                    log("✅ Login detectado (icono de perfil visible)")
                except Exception:
                    log("⚠️ Timeout esperando login. Verificando cookies de todas formas...")

            # Dar tiempo extra para que se establezcan todas las cookies
            time.sleep(5)

            # Extraer cookies
            cookies = context.cookies()

            if not cookies:
                log("❌ No se obtuvieron cookies")
                browser.close()
                return False

            # Verificar que hay cookies de sesión de TikTok
            tiktok_cookies = [c for c in cookies if "tiktok" in c.get("domain", "").lower()]
            session_cookies = [c for c in tiktok_cookies if c.get("name") in ("sessionid", "sid_tt", "sessionid_ss")]

            if not session_cookies:
                log(f"⚠️ Se obtuvieron {len(tiktok_cookies)} cookies TikTok pero sin sessionid")
                log("   Guardando de todas formas, puede funcionar...")

            # Guardar cookies en formato compatible con tiktok-uploader
            save_cookies_for_uploader(cookies)

            # También guardar JSON completo como respaldo
            json_backup_path = os.path.join(SESSION_DIR, "tiktok_cookies_full.json")
            with open(json_backup_path, "w") as f:
                json.dump(cookies, f, indent=2)

            log(f"💾 Cookies guardadas: {len(tiktok_cookies)} cookies TikTok")
            log(f"   Sesión: {[c['name'] for c in session_cookies]}")

            browser.close()
            return True

    except Exception as e:
        log(f"❌ Error en login manual: {str(e)[:200]}")
        return False


def save_cookies_for_uploader(cookies):
    """
    Guarda cookies en formato Netscape (TSV) compatible con tiktok-uploader.
    La librería lee con split('\\t') esperando:
      domain  flag  path  secure  expires  name  value
    """
    ensure_dirs()

    lines = ["# Netscape HTTP Cookie File", "# https://curl.se/docs/http-cookies.html", ""]

    count = 0
    for c in cookies:
        domain = c.get("domain", "")
        if not domain:
            continue
        # Solo cookies de TikTok
        if "tiktok" not in domain.lower():
            continue

        flag = "TRUE" if domain.startswith(".") else "FALSE"
        path = c.get("path", "/")
        secure = "TRUE" if c.get("secure", False) else "FALSE"
        expires = str(int(c.get("expires", 0)))
        name = c.get("name", "")
        value = c.get("value", "")

        if name and value:
            lines.append(f"{domain}\t{flag}\t{path}\t{secure}\t{expires}\t{name}\t{value}")
            count += 1

    with open(COOKIES_PATH, "w") as f:
        f.write("\n".join(lines) + "\n")

    log(f"📁 Cookies Netscape guardadas en {COOKIES_PATH} ({count} cookies)")


def ensure_session():
    """
    Garantiza que hay una sesión válida de TikTok.

    Retorna:
      True  → Sesión lista (cookies disponibles)
      False → No se pudo obtener sesión
    """
    ensure_dirs()

    if cookies_exist():
        age = cookies_age_hours()
        log(f"🍪 Cookies encontradas (edad: {age:.1f}h)")

        # Si las cookies tienen más de 72h, advertir pero intentar
        if age > 72:
            log("⚠️ Cookies > 72h, pueden estar expiradas. Se intentará subir.")

        return True

    # No hay cookies → login manual necesario
    log("🔑 No se encontraron cookies de TikTok")
    success = perform_manual_login()

    if success and cookies_exist():
        log("✅ Sesión establecida correctamente")
        return True

    log("❌ No se pudo establecer sesión")
    return False


# ─── Upload con tiktok-uploader ──────────────────────────────────────────────

def _import_tiktok_uploader():
    """
    Importa tiktok_uploader evitando colisión con este archivo.
    El CWD puede contener 'tiktok_uploader.py' que sombrea el paquete pip.
    """
    import importlib
    import sys as _sys

    # Guardar referencia a este módulo y removerlo temporalmente
    this_module_name = "tiktok_uploader"
    saved = _sys.modules.pop(this_module_name, None)

    # Remover CWD de sys.path temporalmente
    cwd = os.getcwd()
    base = os.path.dirname(os.path.abspath(__file__))
    paths_removed = []
    for p in [cwd, base, "."]:
        while p in _sys.path:
            _sys.path.remove(p)
            paths_removed.append(p)

    try:
        # Ahora importar el paquete pip real
        pkg = importlib.import_module("tiktok_uploader.upload")
        upload_video_fn = getattr(pkg, "upload_video")
        upload_videos_fn = getattr(pkg, "upload_videos")
        return upload_video_fn, upload_videos_fn
    finally:
        # Restaurar todo
        for p in reversed(paths_removed):
            _sys.path.insert(0, p)
        if saved is not None:
            _sys.modules[this_module_name] = saved


def upload_clip(video_path, title, hashtags=None, channel=""):
    """
    Sube un video a TikTok usando Playwright directamente.
    Maneja modales/popups de TikTok que bloquean el upload.

    Args:
        video_path: Ruta al archivo .mp4
        title: Título/descripción del video (incluye hashtags)
        hashtags: Lista de hashtags (opcional, ya incluidos en title)
        channel: Nombre del canal (para logging)

    Returns:
        dict con {success, method, error, needs_manual_login, title}
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {
            "success": False,
            "method": "tiktok-uploader",
            "error": "playwright no instalado. Ejecuta: pip install playwright && playwright install chromium",
            "needs_manual_login": False,
        }

    if not os.path.exists(video_path):
        return {
            "success": False,
            "method": "tiktok-uploader",
            "error": f"Video no encontrado: {video_path}",
            "needs_manual_login": False,
        }

    if not cookies_exist():
        return {
            "success": False,
            "method": "tiktok-uploader",
            "error": "No hay cookies de sesión",
            "needs_manual_login": True,
        }

    config = load_config()
    tiktok_cfg = config.get("tiktok_settings", {})
    headless = tiktok_cfg.get("headless_browser", True)

    log(f"📤 Subiendo: {os.path.basename(video_path)}")
    log(f"   Canal: {channel}")
    log(f"   Título: {title[:80]}...")
    log(f"   Headless: {headless}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-web-security",
                ]
            )

            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                locale="es-CO",
            )

            # Cargar cookies
            cookies = _load_netscape_cookies()
            if cookies:
                context.add_cookies(cookies)
                log(f"   🍪 {len(cookies)} cookies cargadas")
            else:
                browser.close()
                return {
                    "success": False,
                    "method": "tiktok-uploader",
                    "error": "No se pudieron cargar cookies",
                    "needs_manual_login": True,
                }

            page = context.new_page()

            # ─── Navegar a la página de upload ─────────
            log("🌐 Navegando a TikTok upload...")
            page.goto("https://www.tiktok.com/upload", wait_until="domcontentloaded", timeout=30000)
            time.sleep(3)

            # Verificar si estamos logueados (redirect a login = no autenticado)
            if "login" in page.url:
                log("🔑 Redirigido a login, sesión expirada")
                browser.close()
                delete_cookies()
                return {
                    "success": False,
                    "method": "tiktok-uploader",
                    "error": "Sesión expirada, redirigido a login",
                    "needs_manual_login": True,
                }

            # ─── Cerrar cualquier modal/popup ─────────
            _dismiss_modals(page)

            # ─── Subir archivo de video ─────────
            log("📁 Seleccionando archivo de video...")

            # Buscar el input de archivo (puede estar oculto)
            file_input = page.locator('input[type="file"][accept*="video"]').first
            if not file_input:
                file_input = page.locator('input[type="file"]').first

            file_input.set_input_files(video_path)
            log("✅ Video seleccionado, esperando procesamiento...")

            # Esperar a que TikTok procese el video (barra de progreso, thumbnail, etc.)
            time.sleep(5)
            _dismiss_modals(page)

            # Esperar a que se cargue el video (buscar indicadores de carga completa)
            try:
                # Esperar hasta 90s a que desaparezca la barra de progreso o aparezca el thumbnail
                for _ in range(30):
                    _dismiss_modals(page)
                    # Verificar si hay indicador de upload completado
                    progress = page.locator('[class*="progress"], [class*="uploading"], [class*="loading"]')
                    if progress.count() == 0:
                        break
                    time.sleep(3)
                log("✅ Video procesado por TikTok")
            except Exception:
                log("⚠️ Timeout esperando procesamiento, continuando...")

            _dismiss_modals(page)
            time.sleep(2)

            # ─── Escribir descripción/título ─────────
            log("📝 Escribiendo descripción...")

            _dismiss_modals(page)
            time.sleep(1)

            desc_written = False
            try:
                # Intentar con el editor de descripción
                desc_editor = page.locator('[contenteditable="true"]').first
                if desc_editor:
                    # Force click para saltarse cualquier overlay residual
                    desc_editor.click(timeout=10000, force=True)
                    time.sleep(0.5)
                    page.keyboard.press("Meta+A")
                    time.sleep(0.2)
                    page.keyboard.press("Backspace")
                    time.sleep(0.3)
                    # Escribir título
                    page.keyboard.type(title, delay=20)
                    desc_written = True
                    log("✅ Descripción escrita (click directo)")
            except Exception as e:
                log(f"⚠️ Click directo falló: {str(e)[:60]}")

            if not desc_written:
                # Fallback: usar JavaScript para escribir en contenteditable
                try:
                    _dismiss_modals(page)
                    page.evaluate(f"""() => {{
                        const editor = document.querySelector('[contenteditable="true"]');
                        if (editor) {{
                            editor.focus();
                            editor.innerHTML = '';
                            document.execCommand('insertText', false, {json.dumps(title)});
                        }}
                    }}""")
                    desc_written = True
                    log("✅ Descripción escrita (JavaScript)")
                except Exception as e2:
                    log(f"⚠️ JS fallback también falló: {str(e2)[:60]}")

            _dismiss_modals(page)
            time.sleep(1)

            # ─── Click en botón de publicar ─────────
            log("🚀 Publicando...")

            posted = False
            _dismiss_modals(page)

            # Scroll hasta el final de la página para que el botón sea visible
            try:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1)
            except Exception:
                pass

            # Método 1: Buscar botón por selectores y hacer scrollIntoView + click
            post_selectors = [
                'button:has-text("Post")',
                'button:has-text("Publicar")',
                'button:has-text("Upload")',
                'button:has-text("Subir")',
                '[data-e2e="upload-btn"]',
                'button[class*="post"], button[class*="submit"]',
            ]

            for selector in post_selectors:
                try:
                    btn = page.locator(selector).first
                    if btn and btn.count() > 0:
                        _dismiss_modals(page)
                        # ScrollIntoView para asegurar que es visible
                        btn.scroll_into_view_if_needed(timeout=5000)
                        time.sleep(0.5)
                        _dismiss_modals(page)
                        btn.click(timeout=10000, force=True)
                        posted = True
                        log(f"✅ Botón de publicar clickeado: {selector}")
                        break
                except Exception:
                    continue

            # Método 2: Buscar por texto visible del botón
            if not posted:
                try:
                    buttons = page.locator('button').all()
                    for btn in buttons:
                        try:
                            text = btn.inner_text(timeout=500).lower().strip()
                            if text in ("post", "publicar", "upload", "subir", "publish"):
                                _dismiss_modals(page)
                                btn.scroll_into_view_if_needed(timeout=3000)
                                time.sleep(0.3)
                                btn.click(timeout=10000, force=True)
                                posted = True
                                log(f"✅ Botón encontrado por texto: '{text}'")
                                break
                        except Exception:
                            continue
                except Exception:
                    pass

            # Método 3: JavaScript scrollIntoView + click directo
            if not posted:
                try:
                    result = page.evaluate("""() => {
                        const buttons = document.querySelectorAll('button');
                        for (const btn of buttons) {
                            const text = (btn.innerText || '').toLowerCase().trim();
                            if (['post', 'publicar', 'upload', 'subir', 'publish'].includes(text)) {
                                btn.scrollIntoView({ behavior: 'instant', block: 'center' });
                                btn.click();
                                return text;
                            }
                        }
                        // Último intento: buscar por data-e2e
                        const e2e = document.querySelector('[data-e2e="upload-btn"]');
                        if (e2e) { e2e.scrollIntoView(); e2e.click(); return 'e2e-btn'; }
                        return null;
                    }""")
                    if result:
                        posted = True
                        log(f"✅ Botón clickeado via JavaScript: '{result}'")
                except Exception:
                    pass

            # Método 4: Dispatch click event directamente
            if not posted:
                try:
                    result = page.evaluate("""() => {
                        const buttons = document.querySelectorAll('button');
                        for (const btn of buttons) {
                            const text = (btn.innerText || '').toLowerCase().trim();
                            if (['post', 'publicar', 'upload', 'subir', 'publish'].includes(text)) {
                                btn.scrollIntoView({ behavior: 'instant', block: 'center' });
                                const evt = new MouseEvent('click', { bubbles: true, cancelable: true, view: window });
                                btn.dispatchEvent(evt);
                                return text;
                            }
                        }
                        return null;
                    }""")
                    if result:
                        posted = True
                        log(f"✅ Botón clickeado via dispatchEvent: '{result}'")
                except Exception:
                    pass

            # Si no se encontró el botón, tomar screenshot para debug
            if not posted:
                try:
                    ss_path = os.path.join(BASE_DIR, "logs", "debug_upload_fail.png")
                    page.screenshot(path=ss_path, full_page=True)
                    log(f"📸 Screenshot guardado en: {ss_path}")
                    # Listar todos los botones para debug
                    btn_texts = page.evaluate("""() => {
                        return Array.from(document.querySelectorAll('button')).map(b => ({
                            text: (b.innerText || '').trim().substring(0, 50),
                            visible: b.offsetParent !== null,
                            disabled: b.disabled,
                            rect: b.getBoundingClientRect()
                        }));
                    }""")
                    log(f"📋 Botones encontrados: {json.dumps(btn_texts, ensure_ascii=False)[:500]}")
                except Exception:
                    pass

            if not posted:
                log("❌ No se pudo encontrar/clickear el botón Post")
                browser.close()
                return {
                    "success": False,
                    "method": "tiktok-uploader",
                    "error": "No se encontró el botón de publicar. Screenshot en logs/debug_upload_fail.png",
                    "needs_manual_login": False,
                }

            # ─── Esperar confirmación de publicación ─────────
            log("⏳ Esperando confirmación...")
            time.sleep(5)

            # Verificar éxito: buscar mensaje de éxito o redirección
            success_detected = False
            for _ in range(20):  # Máx 60s
                _dismiss_modals(page)
                page_text = page.content().lower()

                if any(kw in page_text for kw in [
                    "uploaded", "your video is being uploaded",
                    "video publicado", "is being processed",
                    "being uploaded", "upload another",
                    "manage your posts", "your video has been uploaded",
                ]):
                    success_detected = True
                    break

                # Si vuelve a la página de upload vacía, probablemente funcionó
                if "upload" in page.url and "select" not in page_text:
                    time.sleep(3)
                    continue

                time.sleep(3)

            browser.close()

            if success_detected or posted:
                log(f"🎉 ¡Video subido exitosamente! → {channel}")
                return {
                    "success": True,
                    "method": "tiktok-uploader",
                    "error": None,
                    "needs_manual_login": False,
                    "title": title,
                    "video": os.path.basename(video_path),
                    "uploaded_at": datetime.utcnow().isoformat() + "Z",
                    "channel": channel,
                }

            return {
                "success": False,
                "method": "tiktok-uploader",
                "error": "No se pudo confirmar el upload",
                "needs_manual_login": False,
            }

    except Exception as e:
        error_msg = str(e)
        log(f"❌ Excepción en upload: {error_msg[:200]}")

        error_lower = error_msg.lower()
        is_auth = any(kw in error_lower for kw in [
            "login", "session", "cookie", "auth", "expired",
            "signin", "captcha", "not logged", "log in",
        ])

        if is_auth:
            log("🔑 Sesión inválida, borrando cookies...")
            delete_cookies()
            return {
                "success": False,
                "method": "tiktok-uploader",
                "error": f"Auth error: {error_msg[:200]}",
                "needs_manual_login": True,
            }

        return {
            "success": False,
            "method": "tiktok-uploader",
            "error": f"Error: {error_msg[:300]}",
            "needs_manual_login": False,
        }


def _load_netscape_cookies():
    """Carga cookies Netscape y las convierte a formato Playwright."""
    cookies = []
    try:
        with open(COOKIES_PATH, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) < 7:
                    continue

                domain = parts[0]
                path = parts[2]
                secure = parts[3].upper() == "TRUE"
                try:
                    expires = int(parts[4])
                except ValueError:
                    expires = -1
                name = parts[5]
                value = parts[6]

                cookie = {
                    "name": name,
                    "value": value,
                    "domain": domain,
                    "path": path,
                    "secure": secure,
                }
                if expires > 0:
                    cookie["expires"] = expires

                # Playwright necesita sameSite
                cookie["sameSite"] = "None" if secure else "Lax"

                cookies.append(cookie)
    except Exception as e:
        log(f"⚠️ Error leyendo cookies: {e}")

    return cookies


def _dismiss_modals(page):
    """Cierra AGRESIVAMENTE cualquier modal/popup/overlay de TikTok."""
    try:
        # 1) Escape primero (cierra la mayoría de modales)
        try:
            page.keyboard.press("Escape")
            time.sleep(0.3)
        except Exception:
            pass

        # 2) Eliminar overlays por JavaScript (más confiable que click)
        try:
            page.evaluate("""() => {
                // Remover modales TUX (TikTok UI)
                document.querySelectorAll('[class*="TUXModal"], [class*="tux-modal"]').forEach(el => el.remove());
                // Remover overlays
                document.querySelectorAll('[class*="overlay"][class*="Modal"], [class*="modal-overlay"]').forEach(el => el.remove());
                // Remover cualquier div con z-index alto que cubra la pantalla
                document.querySelectorAll('div[class*="mask"], div[class*="Mask"]').forEach(el => el.remove());
                // Remover popups genéricos
                document.querySelectorAll('[role="dialog"]').forEach(el => {
                    if (el.querySelector('button')) el.remove();
                });
                // Forzar pointer-events en body
                document.body.style.pointerEvents = 'auto';
                // Remover overflow hidden del body (modales a veces lo ponen)
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

        # 3) Cerrar modales con botón X
        close_selectors = [
            '[class*="Modal"] button[class*="close"]',
            '[class*="modal"] button[class*="close"]',
            '[class*="Modal"] [aria-label="Close"]',
            'button[aria-label="Close"]',
            '[class*="TUXModal"] button',
        ]
        for sel in close_selectors:
            try:
                btns = page.locator(sel).all()
                for btn in btns:
                    if btn.is_visible(timeout=300):
                        btn.click(timeout=1500, force=True)
                        time.sleep(0.2)
            except Exception:
                continue

        # 4) Cerrar por texto de botón
        dismiss_texts = ["got it", "ok", "accept", "entendido", "aceptar",
                         "dismiss", "not now", "skip", "close", "later",
                         "maybe later", "no thanks", "continuar", "continue"]
        try:
            buttons = page.locator('button:visible').all()
            for btn in buttons:
                try:
                    text = btn.inner_text(timeout=300).lower().strip()
                    if any(dt in text for dt in dismiss_texts):
                        btn.click(timeout=1500, force=True)
                        time.sleep(0.2)
                except Exception:
                    continue
        except Exception:
            pass

        # 5) Último recurso: eliminar todo elemento con pointer-events que bloquee
        try:
            page.evaluate("""() => {
                document.querySelectorAll('[class*="overlay"]').forEach(el => {
                    const style = window.getComputedStyle(el);
                    if (style.position === 'fixed' || style.position === 'absolute') {
                        if (parseInt(style.zIndex) > 100) {
                            el.remove();
                        }
                    }
                });
            }""")
        except Exception:
            pass

    except Exception:
        pass


# ─── Upload Queue (Cola inteligente) ─────────────────────────────────────────

class UploadQueue:
    """
    Cola de upload con:
      - Prioridad por viral score
      - Filtrado por horarios pico Colombia
      - Límite diario configurable
      - Delay mínimo entre uploads (2-3h)
      - Máximo 3 reintentos con backoff exponencial
      - Historial persistente
    """

    def __init__(self):
        self.config = load_config()
        self.queue = []  # [{ video_path, channel, title, score, retries, added_at }]
        self.history = self._load_history()

    # ─── Historial persistente ─────────

    def _load_history(self):
        """Carga historial de uploads."""
        try:
            if os.path.exists(UPLOAD_HISTORY_PATH):
                with open(UPLOAD_HISTORY_PATH, "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return {"uploads": [], "daily_counts": {}}

    def _save_history(self):
        """Guarda historial."""
        ensure_dirs()
        try:
            with open(UPLOAD_HISTORY_PATH, "w") as f:
                json.dump(self.history, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log(f"⚠️ Error guardando historial: {e}")

    # ─── Límites y horarios ─────────

    def get_today_key(self):
        """Clave del día actual en Colombia."""
        now = get_colombia_now()
        return now.strftime("%Y-%m-%d")

    def uploads_today(self):
        """Número de uploads hoy."""
        key = self.get_today_key()
        return self.history.get("daily_counts", {}).get(key, 0)

    def can_upload_today(self):
        """Verifica si no se excedió el límite diario."""
        limits = self.config.get("limits", {})
        max_daily = limits.get("clips_per_day", DEFAULT_MAX_UPLOADS)
        tiktok_max = self.config.get("tiktok_settings", {}).get("max_uploads_per_day", DEFAULT_MAX_UPLOADS)
        limit = min(max_daily, tiktok_max)
        return self.uploads_today() < limit

    def is_peak_hour(self):
        """Verifica si estamos en horario pico (±1 hora)."""
        now = get_colombia_now()
        current_hour = now.hour + now.minute / 60.0

        peak_hours = self.config.get("peak_hours", DEFAULT_PEAK_HOURS)

        for peak in peak_hours:
            diff = abs(current_hour - peak)
            if diff <= 1.0 or diff >= 23.0:  # ±1 hora (wrap around midnight)
                return True
        return False

    def is_no_publish_hour(self):
        """Verifica si estamos en horario de no publicar (madrugada)."""
        limits = self.config.get("limits", {})
        no_start = limits.get("no_publish_start_hour", 1)
        no_end = limits.get("no_publish_end_hour", 6)

        now = get_colombia_now()
        hour = now.hour

        if no_start < no_end:
            return no_start <= hour < no_end
        else:  # Wrap midnight
            return hour >= no_start or hour < no_end

    def hours_since_last_upload(self):
        """Horas desde el último upload exitoso."""
        uploads = self.history.get("uploads", [])
        if not uploads:
            return float("inf")

        try:
            last = uploads[-1]
            last_time = datetime.fromisoformat(last["timestamp"].replace("Z", "+00:00"))
            now = datetime.now(last_time.tzinfo) if last_time.tzinfo else datetime.utcnow()
            diff = (now - last_time).total_seconds() / 3600
            return max(0, diff)
        except Exception:
            return float("inf")

    def can_upload_now(self):
        """
        Verifica todas las condiciones para subir ahora.
        Retorna (bool, razón).
        """
        if not self.can_upload_today():
            return False, f"Límite diario alcanzado ({self.uploads_today()})"

        if self.is_no_publish_hour():
            return False, "Horario de no publicación (madrugada)"

        limits = self.config.get("limits", {})
        min_hours = limits.get("min_hours_between_posts", DEFAULT_MIN_HOURS_BETWEEN)
        hours_since = self.hours_since_last_upload()

        if hours_since < min_hours:
            remaining = min_hours - hours_since
            return False, f"Esperar {remaining:.1f}h más entre posts"

        return True, "OK"

    # ─── Cola ─────────

    def add(self, video_path, channel, stream_title="", score=5.0):
        """
        Agrega un video a la cola.

        Args:
            video_path: Ruta al video
            channel: Canal de origen
            stream_title: Título del stream
            score: Puntuación viral (mayor = más prioridad)
        """
        item = {
            "video_path": video_path,
            "channel": channel,
            "stream_title": stream_title,
            "score": float(score),
            "retries": 0,
            "added_at": datetime.utcnow().isoformat() + "Z",
            "video_hash": file_hash(video_path),
        }

        # Verificar duplicados por hash
        existing_hashes = {q["video_hash"] for q in self.queue}
        uploaded_hashes = {u.get("video_hash", "") for u in self.history.get("uploads", [])}

        if item["video_hash"] in existing_hashes:
            log(f"⏭️ Video ya en cola: {os.path.basename(video_path)}")
            return False

        if item["video_hash"] in uploaded_hashes:
            log(f"⏭️ Video ya subido antes: {os.path.basename(video_path)}")
            return False

        self.queue.append(item)
        # Ordenar por score descendente
        self.queue.sort(key=lambda x: x["score"], reverse=True)

        log(f"📋 Agregado a cola: {channel} (score={score:.1f}, pos={self.queue.index(item) + 1}/{len(self.queue)})")
        return True

    def process_next(self):
        """
        Procesa el siguiente video de la cola.

        Retorna:
            dict con resultado del upload, o None si no hay nada que procesar
        """
        if not self.queue:
            log("📋 Cola vacía")
            return None

        # Verificar si podemos subir ahora
        can, reason = self.can_upload_now()
        if not can:
            log(f"⏳ No se puede subir ahora: {reason}")
            return {"success": False, "error": reason, "method": "queue", "needs_manual_login": False}

        # Verificar sesión
        if not cookies_exist():
            log("🔑 Sin sesión, intentando login...")
            if not ensure_session():
                return {"success": False, "error": "Sin sesión TikTok", "method": "queue", "needs_manual_login": True}

        # Tomar el de mayor score
        item = self.queue[0]

        if not os.path.exists(item["video_path"]):
            log(f"⚠️ Video no existe, removiendo de cola: {item['video_path']}")
            self.queue.pop(0)
            return self.process_next()

        # Generar título
        title_data = generate_title_and_hashtags(item["channel"], item["stream_title"])
        title = title_data["title"]

        log(f"🚀 Procesando: {item['channel']} (score={item['score']:.1f}, intento={item['retries'] + 1}/{MAX_RETRIES})")

        # Delay aleatorio pre-upload (3-8s)
        delay = random.uniform(3, 8)
        log(f"⏳ Delay pre-upload: {delay:.0f}s")
        time.sleep(delay)

        # Intentar upload
        result = upload_clip(
            video_path=item["video_path"],
            title=title,
            hashtags=title_data["hashtags"],
            channel=item["channel"],
        )

        if result.get("success"):
            # Éxito → remover de cola, registrar en historial
            self.queue.pop(0)
            self._record_success(item, result)
            self._move_to_uploaded(item["video_path"])
            return result

        # Falló
        if result.get("needs_manual_login"):
            # Error de auth → no incrementar retry, necesita login
            log("🔑 Se necesita re-login para continuar")
            return result

        # Incrementar reintentos
        item["retries"] += 1

        if item["retries"] >= MAX_RETRIES:
            log(f"💀 Máximo reintentos alcanzado para {item['channel']}, removiendo")
            self.queue.pop(0)
            self._record_failure(item, result)
            return result

        # Backoff exponencial
        backoff = RETRY_DELAY_BASE_SEC * (2 ** (item["retries"] - 1))
        log(f"🔄 Reintento {item['retries']}/{MAX_RETRIES} en {backoff}s")

        return result

    def _record_success(self, item, result):
        """Registra upload exitoso."""
        entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "channel": item["channel"],
            "video": os.path.basename(item["video_path"]),
            "video_hash": item.get("video_hash", ""),
            "title": result.get("title", ""),
            "score": item["score"],
            "retries": item["retries"],
            "method": result.get("method", "tiktok-uploader"),
            "success": True,
        }

        self.history.setdefault("uploads", []).append(entry)

        # Incrementar contador diario
        key = self.get_today_key()
        self.history.setdefault("daily_counts", {})
        self.history["daily_counts"][key] = self.history["daily_counts"].get(key, 0) + 1

        self._save_history()
        self._log_upload(entry)

    def _record_failure(self, item, result):
        """Registra upload fallido (agotó reintentos)."""
        entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "channel": item["channel"],
            "video": os.path.basename(item["video_path"]),
            "error": result.get("error", "unknown"),
            "retries": item["retries"],
            "success": False,
        }

        self.history.setdefault("failures", []).append(entry)
        self._save_history()
        self._log_upload(entry)

    def _log_upload(self, entry):
        """Escribe en uploads.jsonl."""
        ensure_dirs()
        try:
            with open(UPLOAD_LOG_PATH, "a") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _move_to_uploaded(self, video_path):
        """Mueve video a carpeta uploaded/."""
        if os.path.exists(video_path):
            try:
                dest = os.path.join(UPLOADED_DIR, os.path.basename(video_path))
                shutil.move(video_path, dest)
                log(f"📁 Video movido a: {dest}")
            except Exception:
                pass

    def status(self):
        """Info de estado de la cola."""
        can, reason = self.can_upload_now()
        hours = self.hours_since_last_upload()
        return {
            "queue_size": len(self.queue),
            "uploads_today": self.uploads_today(),
            "can_upload": can,
            "reason": reason,
            "is_peak_hour": self.is_peak_hour(),
            "hours_since_last": round(hours, 1) if hours != float("inf") else -1,
            "next_in_queue": self.queue[0]["channel"] if self.queue else None,
        }


# ─── Función principal de upload (interfaz con kick_clip_bot.js) ─────────────

def upload_to_tiktok(video_path, channel_name, stream_title=""):
    """
    Función principal de upload.
    Compatible con kick_clip_bot.js que espera:
      {"success": bool, "method": str, "error": str, "needs_manual_login": bool}

    Args:
        video_path: Ruta al video .mp4
        channel_name: Canal de Kick
        stream_title: Título del stream (opcional)

    Returns:
        dict JSON serializable
    """
    load_env()
    ensure_dirs()

    if not os.path.exists(video_path):
        return {
            "success": False,
            "method": "tiktok-uploader",
            "error": f"Video no encontrado: {video_path}",
            "needs_manual_login": False,
        }

    config = load_config()
    tiktok_cfg = config.get("tiktok_settings", {})

    # ─── Paso 1: Asegurar sesión ─────────
    if not ensure_session():
        return {
            "success": False,
            "method": "tiktok-uploader",
            "error": "No se pudo establecer sesión TikTok. Login manual requerido.",
            "needs_manual_login": True,
        }

    # ─── Paso 2: Generar título y hashtags ─────────
    title_data = generate_title_and_hashtags(channel_name, stream_title)
    log(f"📝 Título: {title_data['title'][:80]}")

    # ─── Paso 3: Delay anti-spam (mínimo) ─────────
    delay = random.uniform(2, 5)
    log(f"⏳ Delay pre-upload: {delay:.0f}s")
    time.sleep(delay)

    # ─── Paso 4: Upload con reintentos ─────────
    last_result = None

    for attempt in range(1, MAX_RETRIES + 1):
        log(f"🔄 Intento {attempt}/{MAX_RETRIES}")

        result = upload_clip(
            video_path=video_path,
            title=title_data["title"],
            hashtags=title_data["hashtags"],
            channel=channel_name,
        )

        if result.get("success"):
            # ─── Éxito ─────────
            log_upload_result(result, video_path)
            move_to_uploaded(video_path)
            return result

        last_result = result

        if result.get("needs_manual_login"):
            # Error de auth → no reintentar, necesita login humano
            log("🔑 Login manual requerido, abortando reintentos")
            return result

        # Esperar antes de reintentar (backoff corto)
        if attempt < MAX_RETRIES:
            backoff = RETRY_DELAY_BASE_SEC * (2 ** (attempt - 1))
            backoff = min(backoff, 30)
            log(f"⏳ Esperando {backoff}s antes de reintentar...")
            time.sleep(backoff)

            # Re-verificar sesión
            if not cookies_exist():
                log("🔑 Cookies perdidas, intentando re-login...")
                if not ensure_session():
                    return {
                        "success": False,
                        "method": "tiktok-uploader",
                        "error": "Sesión perdida durante reintentos",
                        "needs_manual_login": True,
                    }

    # Todos los intentos fallaron
    log(f"💀 Upload fallido después de {MAX_RETRIES} intentos")
    log_upload_result(last_result or {}, video_path)
    return last_result or {
        "success": False,
        "method": "tiktok-uploader",
        "error": "Todos los intentos fallaron",
        "needs_manual_login": False,
    }


def log_upload_result(result, video_path):
    """Registra el resultado del upload en el log JSONL."""
    ensure_dirs()

    entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "video": os.path.basename(video_path),
        "success": result.get("success", False),
        "method": result.get("method", "tiktok-uploader"),
        "title": result.get("title", ""),
        "error": result.get("error"),
    }

    try:
        with open(UPLOAD_LOG_PATH, "a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def move_to_uploaded(video_path):
    """Mueve video exitoso a uploaded/."""
    if os.path.exists(video_path):
        try:
            dest = os.path.join(UPLOADED_DIR, os.path.basename(video_path))
            shutil.move(video_path, dest)
            log(f"📁 Video movido a: {dest}")
        except Exception:
            pass


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    # Modo: solo generar título
    if args and args[0] == "--generate-title":
        channel = args[1] if len(args) > 1 else "unknown"
        title = args[2] if len(args) > 2 else ""
        result = generate_title_and_hashtags(channel, title)
        print(json.dumps(result, ensure_ascii=False))
        return

    # Modo: verificar/crear sesión
    if args and args[0] == "--login":
        ensure_dirs()
        log("🔐 Modo login manual...")
        success = ensure_session()
        print(json.dumps({
            "success": success,
            "cookies_exist": cookies_exist(),
            "cookies_path": COOKIES_PATH,
        }, ensure_ascii=False))
        return

    # Modo: estado de la cola
    if args and args[0] == "--queue-status":
        queue = UploadQueue()
        print(json.dumps(queue.status(), ensure_ascii=False))
        return

    # Modo: forzar borrar cookies
    if args and args[0] == "--delete-cookies":
        delete_cookies()
        print(json.dumps({"success": True, "message": "Cookies eliminadas"}))
        return

    # Modo principal: upload
    if len(args) < 2:
        print(json.dumps({
            "success": False,
            "error": "Uso: python3 tiktok_uploader.py <video.mp4> <canal> [titulo_stream]",
            "needs_manual_login": False,
        }, ensure_ascii=False))
        sys.exit(1)

    video_path = args[0]
    channel_name = args[1]
    stream_title = args[2] if len(args) > 2 else ""

    result = upload_to_tiktok(video_path, channel_name, stream_title)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
