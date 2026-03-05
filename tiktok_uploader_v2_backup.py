#!/usr/bin/env python3
"""
KickClipBot — tiktok_uploader.py
═══════════════════════════════════════════════════════════════
Sube videos procesados a TikTok automáticamente.

Métodos:
  1. Playwright (navegador headless) — método principal
  2. TikTok Content Posting API v2 — si tienes credenciales API

Uso:
    python3 tiktok_uploader.py <video.mp4> <canal> [titulo_stream]

Salida: JSON con resultado del upload
"""

import json
import sys
import os
import time
import random
import re
from datetime import datetime

# ─── Configuración ────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
ENV_PATH = os.path.join(SCRIPT_DIR, ".env")


def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def load_env():
    """Carga variables de .env"""
    env_vars = {}
    try:
        with open(ENV_PATH, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    env_vars[key.strip()] = val.strip()
    except FileNotFoundError:
        pass
    # Merge con os.environ (env vars del sistema tienen prioridad)
    for key, val in env_vars.items():
        if key not in os.environ or not os.environ[key]:
            os.environ[key] = val
    return env_vars


def ensure_dirs():
    config = load_config()
    paths = config.get("paths", {})
    uploaded_dir = os.path.join(SCRIPT_DIR, paths.get("uploaded_dir", "./uploaded"))
    cookies_dir = os.path.join(SCRIPT_DIR, paths.get("cookies_dir", "./cookies"))
    logs_dir = os.path.join(SCRIPT_DIR, paths.get("logs_dir", "./logs"))
    os.makedirs(uploaded_dir, exist_ok=True)
    os.makedirs(cookies_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)
    return {"uploaded": uploaded_dir, "cookies": cookies_dir, "logs": logs_dir}


# ─── Generador de título y hashtags ───────────────────────────────────────────

def generate_title_and_hashtags(channel_name, stream_title="", extra_tags=None):
    """
    Genera título y hashtags optimizados para TikTok.
    """
    config = load_config()
    tiktok_cfg = config.get("tiktok_settings", {})
    default_tags = tiktok_cfg.get("hashtags_default", ["kick", "clips", "viral", "gaming", "stream"])
    max_length = tiktok_cfg.get("title_max_length", 150)

    # Limpiar título del stream
    clean_title = stream_title.strip() if stream_title else ""
    # Remover emojis excesivos y caracteres especiales problemáticos
    clean_title = re.sub(r'[^\w\s\-!?¿¡.,@#áéíóúñÁÉÍÓÚÑ]', '', clean_title).strip()

    # Hashtags del canal
    channel_tags = [channel_name.lower()]

    # Tags contextuales basados en el título
    context_tags = []
    title_lower = (clean_title or "").lower()
    gaming_keywords = {
        "fortnite": ["fortnite", "battle royale"],
        "valorant": ["valorant", "fps"],
        "minecraft": ["minecraft"],
        "gta": ["gta", "gtav"],
        "league": ["lol", "leagueoflegends"],
        "fifa": ["fifa", "futbol"],
        "call of duty": ["cod", "warzone"],
        "apex": ["apex", "apexlegends"],
        "just chatting": ["justchatting", "irl"],
    }
    for keyword, tags in gaming_keywords.items():
        if keyword in title_lower:
            context_tags.extend(tags)

    # Construir lista de hashtags (sin duplicados)
    all_tags = []
    seen = set()
    for tag in channel_tags + context_tags + default_tags + (extra_tags or []):
        tag_clean = tag.lower().replace(" ", "").replace("#", "")
        if tag_clean and tag_clean not in seen:
            seen.add(tag_clean)
            all_tags.append(f"#{tag_clean}")

    # Construir título
    if clean_title:
        title_base = f"🔴 {clean_title} | @{channel_name} en Kick"
    else:
        title_base = f"🔴 Clip de @{channel_name} en Kick"

    hashtags_str = " ".join(all_tags)

    # Recortar si excede el límite
    full_title = f"{title_base} {hashtags_str}"
    if len(full_title) > max_length:
        # Primero recortar hashtags
        available = max_length - len(title_base) - 1
        if available > 20:
            trimmed_tags = ""
            for tag in all_tags:
                if len(trimmed_tags) + len(tag) + 1 <= available:
                    trimmed_tags += f" {tag}"
                else:
                    break
            full_title = f"{title_base}{trimmed_tags}"
        else:
            full_title = title_base[:max_length]

    return {
        "title": full_title,
        "hashtags": all_tags,
        "channel": channel_name,
    }


# ─── Método 1: Upload via Playwright (navegador) ─────────────────────────────

def upload_via_playwright(video_path, title_data):
    """
    Sube video a TikTok usando Playwright (navegador headless).
    Requiere: pip install playwright && playwright install chromium
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {
            "error": "playwright no instalado. Ejecuta: pip install playwright && python -m playwright install chromium",
            "method": "playwright",
            "success": False,
        }

    config = load_config()
    tiktok_cfg = config.get("tiktok_settings", {})
    headless = tiktok_cfg.get("headless_browser", True)
    dirs = ensure_dirs()

    email = os.environ.get("TIKTOK_EMAIL", "")
    password = os.environ.get("TIKTOK_PASSWORD", "")

    if not email or not password:
        return {
            "error": "Credenciales TikTok no configuradas. Configura TIKTOK_EMAIL y TIKTOK_PASSWORD en .env",
            "method": "playwright",
            "success": False,
        }

    cookies_file = os.path.join(dirs["cookies"], "tiktok_state.json")
    title = title_data.get("title", "Clip de Kick")

    try:
        with sync_playwright() as p:
            # Usar contexto persistente para mantener cookies
            browser_args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]

            if os.path.exists(cookies_file):
                # Reusar sesión existente
                context = p.chromium.launch_persistent_context(
                    user_data_dir=os.path.join(dirs["cookies"], "tiktok_browser_data"),
                    headless=headless,
                    args=browser_args,
                    viewport={"width": 1280, "height": 900},
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                )
                page = context.pages[0] if context.pages else context.new_page()
            else:
                context = p.chromium.launch_persistent_context(
                    user_data_dir=os.path.join(dirs["cookies"], "tiktok_browser_data"),
                    headless=headless,
                    args=browser_args,
                    viewport={"width": 1280, "height": 900},
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                )
                page = context.new_page()

            # Navegar al upload de TikTok
            page.goto("https://www.tiktok.com/upload", wait_until="networkidle", timeout=60000)
            time.sleep(3)

            # Verificar si necesitamos login
            current_url = page.url
            if "login" in current_url.lower() or page.locator("text=Log in").count() > 0:
                print("[tiktok_uploader] Iniciando sesión en TikTok...", file=sys.stderr)

                # Intentar login con email
                page.goto("https://www.tiktok.com/login/phone-or-email/email", wait_until="networkidle", timeout=60000)
                time.sleep(2)

                # Rellenar email
                email_input = page.locator('input[name="username"], input[placeholder*="email"], input[type="text"]').first
                if email_input.count() > 0:
                    email_input.click()
                    email_input.fill(email)
                    time.sleep(random.uniform(0.5, 1.0))

                # Rellenar password
                pwd_input = page.locator('input[type="password"]').first
                if pwd_input.count() > 0:
                    pwd_input.click()
                    pwd_input.fill(password)
                    time.sleep(random.uniform(0.5, 1.0))

                # Click login
                login_btn = page.locator('button[type="submit"], button:has-text("Log in")').first
                if login_btn.count() > 0:
                    login_btn.click()

                # Esperar a que cargue (puede haber CAPTCHA)
                print("[tiktok_uploader] Esperando login (puede requerir CAPTCHA manual)...", file=sys.stderr)
                time.sleep(10)

                # Verificar si el login fue exitoso
                page.goto("https://www.tiktok.com/upload", wait_until="networkidle", timeout=60000)
                time.sleep(3)

                if "login" in page.url.lower():
                    # Guardar screenshot para debug
                    screenshot_path = os.path.join(dirs["logs"], f"login_fail_{int(time.time())}.png")
                    page.screenshot(path=screenshot_path)
                    context.close()
                    return {
                        "error": f"Login falló. Puede requerir CAPTCHA manual. Screenshot: {screenshot_path}",
                        "method": "playwright",
                        "success": False,
                        "needs_manual_login": True,
                    }

                print("[tiktok_uploader] Login exitoso", file=sys.stderr)

            # ── Upload del video ──
            print(f"[tiktok_uploader] Subiendo video: {os.path.basename(video_path)}", file=sys.stderr)

            # Buscar input de archivo
            file_input = page.locator('input[type="file"]').first
            if file_input.count() == 0:
                # Esperar a que aparezca
                page.wait_for_selector('input[type="file"]', timeout=30000)
                file_input = page.locator('input[type="file"]').first

            # Subir archivo
            file_input.set_input_files(video_path)
            print("[tiktok_uploader] Archivo seleccionado, esperando procesamiento...", file=sys.stderr)

            # Esperar procesamiento del video (hasta 2 min)
            time.sleep(10)

            # Rellenar título/descripción
            try:
                # TikTok usa un editor de texto enriquecido
                caption_editor = page.locator('[data-text="true"], .public-DraftEditor-content, [contenteditable="true"]').first
                if caption_editor.count() > 0:
                    caption_editor.click()
                    # Limpiar texto existente
                    page.keyboard.press("Control+a")
                    time.sleep(0.3)
                    page.keyboard.press("Backspace")
                    time.sleep(0.3)
                    # Escribir título
                    page.keyboard.type(title, delay=30)
                    time.sleep(1)
                else:
                    # Intentar con textarea
                    textarea = page.locator('textarea, input[placeholder*="caption"], input[placeholder*="descripción"]').first
                    if textarea.count() > 0:
                        textarea.fill(title)
            except Exception as e:
                print(f"[tiktok_uploader] Aviso: No se pudo rellenar título: {str(e)[:100]}", file=sys.stderr)

            # Esperar a que el video se procese completamente
            print("[tiktok_uploader] Esperando que TikTok procese el video...", file=sys.stderr)
            max_wait = 120  # 2 minutos máximo
            waited = 0
            while waited < max_wait:
                # Buscar indicadores de que el video está listo
                if page.locator('text="Uploaded"').count() > 0 or \
                   page.locator('[class*="upload-success"]').count() > 0 or \
                   page.locator('[class*="progress-complete"]').count() > 0:
                    break
                time.sleep(5)
                waited += 5

            time.sleep(3)

            # Click en "Post" / "Publicar"
            post_btn = page.locator('button:has-text("Post"), button:has-text("Publicar"), button[class*="post-button"]').first
            if post_btn.count() > 0 and post_btn.is_enabled():
                post_btn.click()
                print("[tiktok_uploader] Click en publicar...", file=sys.stderr)
                time.sleep(10)

                # Verificar éxito
                if page.locator('text="Your video has been uploaded"').count() > 0 or \
                   page.locator('text="uploaded"').count() > 0 or \
                   "manage" in page.url.lower():
                    context.close()
                    return {
                        "success": True,
                        "method": "playwright",
                        "title": title,
                        "video": os.path.basename(video_path),
                        "uploaded_at": datetime.utcnow().isoformat() + "Z",
                        "error": None,
                    }

            # Si llegamos aquí, intentar capturar el estado
            screenshot_path = os.path.join(dirs["logs"], f"upload_state_{int(time.time())}.png")
            page.screenshot(path=screenshot_path)

            context.close()
            return {
                "success": False,
                "method": "playwright",
                "error": f"No se pudo confirmar el upload. Screenshot guardado: {screenshot_path}",
                "screenshot": screenshot_path,
                "title": title,
            }

    except Exception as e:
        return {
            "success": False,
            "method": "playwright",
            "error": f"Error Playwright: {str(e)[:300]}",
        }


# ─── Método 2: TikTok Content Posting API v2 ─────────────────────────────────

def upload_via_api(video_path, title_data):
    """
    Sube video usando TikTok Content Posting API v2.
    Requiere: access_token de OAuth2 con scopes correctos.
    Docs: https://developers.tiktok.com/doc/content-posting-api-get-started
    """
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        return {"error": "curl_cffi no instalado", "method": "api", "success": False}

    access_token = os.environ.get("TIKTOK_ACCESS_TOKEN", "")
    if not access_token:
        return {
            "error": "TIKTOK_ACCESS_TOKEN no configurado en .env (requiere OAuth2 App)",
            "method": "api",
            "success": False,
        }

    title = title_data.get("title", "Clip de Kick")
    file_size = os.path.getsize(video_path)

    session = cffi_requests.Session(impersonate="chrome131")

    try:
        # Paso 1: Iniciar upload (Source Upload)
        print("[tiktok_uploader] API: Iniciando upload...", file=sys.stderr)

        init_url = "https://open.tiktokapis.com/v2/post/publish/inbox/video/init/"
        init_payload = {
            "source_info": {
                "source": "FILE_UPLOAD",
                "video_size": file_size,
                "chunk_size": min(file_size, 10 * 1024 * 1024),  # 10MB chunks
                "total_chunk_count": max(1, (file_size + 10 * 1024 * 1024 - 1) // (10 * 1024 * 1024)),
            }
        }

        init_res = session.post(
            init_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=UTF-8",
            },
            json=init_payload,
            timeout=30,
        )

        if init_res.status_code != 200:
            return {
                "error": f"API init falló: HTTP {init_res.status_code} - {init_res.text[:200]}",
                "method": "api",
                "success": False,
            }

        init_data = init_res.json()
        if init_data.get("error", {}).get("code") != "ok":
            return {
                "error": f"API init error: {init_data.get('error', {})}",
                "method": "api",
                "success": False,
            }

        publish_id = init_data.get("data", {}).get("publish_id", "")
        upload_url = init_data.get("data", {}).get("upload_url", "")

        if not upload_url:
            return {"error": "API no retornó upload_url", "method": "api", "success": False}

        # Paso 2: Subir el video
        print("[tiktok_uploader] API: Subiendo video...", file=sys.stderr)

        with open(video_path, "rb") as f:
            video_data = f.read()

        chunk_size = 10 * 1024 * 1024  # 10MB
        total_chunks = max(1, (file_size + chunk_size - 1) // chunk_size)

        for chunk_idx in range(total_chunks):
            start = chunk_idx * chunk_size
            end = min(start + chunk_size, file_size)
            chunk = video_data[start:end]

            upload_res = session.put(
                upload_url,
                headers={
                    "Content-Range": f"bytes {start}-{end - 1}/{file_size}",
                    "Content-Type": "video/mp4",
                },
                data=chunk,
                timeout=120,
            )

            if upload_res.status_code not in (200, 201, 206):
                return {
                    "error": f"Upload chunk {chunk_idx + 1}/{total_chunks} falló: HTTP {upload_res.status_code}",
                    "method": "api",
                    "success": False,
                }

            print(f"[tiktok_uploader] API: Chunk {chunk_idx + 1}/{total_chunks} subido", file=sys.stderr)

        # Paso 3: Publicar el video
        print("[tiktok_uploader] API: Publicando...", file=sys.stderr)

        publish_url = "https://open.tiktokapis.com/v2/post/publish/video/init/"
        publish_payload = {
            "post_info": {
                "title": title[:150],
                "privacy_level": "PUBLIC_TO_EVERYONE",
                "disable_duet": False,
                "disable_stitch": False,
                "disable_comment": False,
                "video_cover_timestamp_ms": 1000,
            },
            "source_info": {
                "source": "PULL_FROM_URL",
                "video_url": upload_url,
            },
        }

        pub_res = session.post(
            publish_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=UTF-8",
            },
            json=publish_payload,
            timeout=60,
        )

        if pub_res.status_code == 200:
            pub_data = pub_res.json()
            if pub_data.get("error", {}).get("code") == "ok":
                return {
                    "success": True,
                    "method": "api",
                    "publish_id": pub_data.get("data", {}).get("publish_id", publish_id),
                    "title": title,
                    "video": os.path.basename(video_path),
                    "uploaded_at": datetime.utcnow().isoformat() + "Z",
                    "error": None,
                }

        return {
            "success": False,
            "method": "api",
            "error": f"Publish falló: HTTP {pub_res.status_code} - {pub_res.text[:200]}",
            "publish_id": publish_id,
        }

    except Exception as e:
        return {
            "success": False,
            "method": "api",
            "error": f"Error API: {str(e)[:300]}",
        }


# ─── Upload principal (intenta API primero, luego Playwright) ─────────────────

def upload_to_tiktok(video_path, channel_name, stream_title=""):
    """
    Función principal de upload.
    Intenta API primero, si falla usa Playwright.
    """
    load_env()

    if not os.path.exists(video_path):
        return {"error": f"Video no encontrado: {video_path}", "success": False}

    config = load_config()
    tiktok_cfg = config.get("tiktok_settings", {})
    use_api = tiktok_cfg.get("use_api", False)

    # Generar título y hashtags
    title_data = generate_title_and_hashtags(channel_name, stream_title)
    print(f"[tiktok_uploader] Título: {title_data['title']}", file=sys.stderr)

    # Delay aleatorio para evitar detección de spam
    delay_min = tiktok_cfg.get("upload_delay_min_sec", 5)
    delay_max = tiktok_cfg.get("upload_delay_max_sec", 15)
    delay = random.uniform(delay_min, min(delay_max, 30))  # Máx 30s para CLI directo
    print(f"[tiktok_uploader] Delay pre-upload: {delay:.0f}s", file=sys.stderr)
    time.sleep(delay)

    result = None

    # Intentar API si está configurado
    if use_api and os.environ.get("TIKTOK_ACCESS_TOKEN"):
        print("[tiktok_uploader] Intentando upload via API...", file=sys.stderr)
        result = upload_via_api(video_path, title_data)
        if result.get("success"):
            log_upload(result, video_path)
            return result
        print(f"[tiktok_uploader] API falló: {result.get('error', '?')[:100]}", file=sys.stderr)

    # Fallback: Playwright
    print("[tiktok_uploader] Usando Playwright para upload...", file=sys.stderr)
    result = upload_via_playwright(video_path, title_data)

    log_upload(result, video_path)
    return result


def log_upload(result, video_path):
    """Registra el resultado del upload en el log."""
    dirs = ensure_dirs()
    log_file = os.path.join(dirs["logs"], "uploads.jsonl")

    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "video": os.path.basename(video_path),
        "success": result.get("success", False),
        "method": result.get("method", "unknown"),
        "title": result.get("title", ""),
        "error": result.get("error"),
    }

    try:
        with open(log_file, "a") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    except Exception:
        pass

    # Si fue exitoso, mover video a uploaded/
    if result.get("success") and os.path.exists(video_path):
        try:
            import shutil
            dest = os.path.join(dirs["uploaded"], os.path.basename(video_path))
            shutil.move(video_path, dest)
            print(f"[tiktok_uploader] Video movido a: {dest}", file=sys.stderr)
        except Exception:
            pass


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    if len(args) < 2:
        print(json.dumps({
            "error": "Uso: python3 tiktok_uploader.py <video.mp4> <canal> [titulo_stream]"
        }))
        sys.exit(1)

    video_path = args[0]
    channel_name = args[1]
    stream_title = args[2] if len(args) > 2 else ""

    if args[0] == "--generate-title":
        # Solo generar título sin subir
        channel = args[1] if len(args) > 1 else "unknown"
        title = args[2] if len(args) > 2 else ""
        result = generate_title_and_hashtags(channel, title)
        print(json.dumps(result, ensure_ascii=False))
        return

    result = upload_to_tiktok(video_path, channel_name, stream_title)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
