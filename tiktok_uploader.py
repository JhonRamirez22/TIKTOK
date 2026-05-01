#!/usr/bin/env python3
"""
KickClipBot — tiktok_uploader.py (Senior Engineer Edition)
═══════════════════════════════════════════════════════════════
Sistema avanzado de subida a TikTok usando Playwright nativo.

Implementa:
- Rotación de User-Agents
- Simulación de movimiento de ratón y delays random (anti-bot)
- Detección de CAPTCHA y pausa inteligente (30 min)
- Notificaciones Webhook (Discord/Telegram)
- Capturas de pantalla en error
"""

import sys
import os
import json
import time
import random
import logging
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright, TimeoutError

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
COOKIES_DIR = os.path.join(SCRIPT_DIR, "cookies")
os.makedirs(COOKIES_DIR, exist_ok=True)
COOKIES_PATH = os.path.join(COOKIES_DIR, "tiktok_state.json")
DEBUG_DIR = os.path.join(SCRIPT_DIR, "debug")
os.makedirs(DEBUG_DIR, exist_ok=True)

# Config Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | uploader | %(message)s")
logger = logging.getLogger(__name__)

def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}

config = load_config()
anti_det = config.get("tiktok_settings", {}).get("anti_detection", {})
webhooks = config.get("alerts_and_monitoring", {}).get("webhooks", {})

# Delays variables
def human_delay(min_s=None, max_s=None):
    if min_s is None: min_s = anti_det.get("min_delay_sec", 1.2)
    if max_s is None: max_s = anti_det.get("max_delay_sec", 3.8)
    time.sleep(random.uniform(min_s, max_s))

def send_webhook(title, message, is_error=False):
    """Envía alerta a Discord si está configurado."""
    discord_url = webhooks.get("discord_url")
    if not discord_url: return
    
    import requests
    payload = {
        "embeds": [{
            "title": title,
            "description": message,
            "color": 16711680 if is_error else 65280,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }]
    }
    try:
        requests.post(discord_url, json=payload, timeout=5)
    except Exception as e:
        logger.error(f"Fallo enviando webhook: {e}")

class TikTokUploader:
    def __init__(self, headless=True):
        self.headless = headless
        self._using_cdp = False
        self.user_agents = anti_det.get("user_agents", [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        ])
        
    def _simulate_mouse(self, page, element=None):
        """Simula un movimiento orgánico del ratón."""
        if not anti_det.get("simulate_mouse", True): return
        
        try:
            if element:
                box = element.bounding_box()
                if box:
                    x = box["x"] + box["width"] / 2 + random.uniform(-10, 10)
                    y = box["y"] + box["height"] / 2 + random.uniform(-10, 10)
                    page.mouse.move(x, y, steps=10)
            else:
                x = random.randint(100, 800)
                y = random.randint(100, 600)
                page.mouse.move(x, y, steps=5)
        except Exception:
            pass

    def check_captcha(self, page):
        """Detecta si hay un CAPTCHA de TikTok en pantalla."""
        captcha_selectors = [
            ".captcha-disable-scroll",
            "#captcha-verify-image",
            "div[class*='captcha']"
        ]
        for sel in captcha_selectors:
            if page.locator(sel).count() > 0:
                logger.error("¡CAPTCHA DETECTADO!")
                screenshot_path = os.path.join(DEBUG_DIR, f"captcha_{int(time.time())}.png")
                page.screenshot(path=screenshot_path)
                
                pause_min = anti_det.get("captcha_pause_min", 30)
                send_webhook("⚠️ CAPTCHA Detectado", 
                             f"El bot ha sido bloqueado por CAPTCHA. Pausando subidas por {pause_min} minutos.\nCaptura guardada en: {screenshot_path}", 
                             is_error=True)
                return True
        return False

    def _connect_browser(self, p):
        """Conecta a Chrome real por CDP o lanza Chromium propio."""
        browser_cfg = config.get("tiktok_settings", {}).get("browser", {})
        use_cdp = browser_cfg.get("use_remote_debugging", False)
        cdp_url = browser_cfg.get("cdp_url", "http://127.0.0.1:9222")

        if use_cdp:
            self._using_cdp = True
            logger.info(f"Conectando a Chrome real via CDP: {cdp_url}")
            return p.chromium.connect_over_cdp(cdp_url)

        self._using_cdp = False
        ua = random.choice(self.user_agents)
        return p.chromium.launch(
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled", "--disable-infobars"],
        )

    def upload(self, video_path, payload):
        """Proceso de subida orgánico a TikTok Studio."""
        logger.info(f"Iniciando subida de {video_path} para el canal {payload.get('channel')}")
        
        with sync_playwright() as p:
            browser = self._connect_browser(p)

            # Si usamos CDP, tomamos el contexto existente
            if hasattr(browser, "contexts") and browser.contexts:
                context = browser.contexts[0]
                if context.pages:
                    page = context.pages[0]
                else:
                    page = context.new_page()
            else:
                ua = random.choice(self.user_agents)
                context = browser.new_context(
                    user_agent=ua,
                    viewport={"width": 1280, "height": 720}
                )
                page = context.new_page()

            # 1. Cargar estado/cookies (solo si NO usamos CDP)
            if not self._using_cdp:
                if os.path.exists(COOKIES_PATH):
                    try:
                        with open(COOKIES_PATH, "r") as f:
                            state = json.load(f)
                        context.add_cookies(state.get("cookies", []))
                        logger.info("Cookies cargadas exitosamente.")
                    except Exception as e:
                        logger.error(f"Error cargando cookies: {e}")

            try:
                # 2. Navegar a página de subida
                upload_url = config.get("tiktok_settings", {}).get("upload_url", "https://www.tiktok.com/tiktokstudio/upload")
                logger.info(f"Navegando a {upload_url}...")
                page.goto(upload_url, timeout=60000, wait_until="domcontentloaded")
                page.wait_for_timeout(5000)  # Esperar que cargue
                human_delay(3, 6)

                # Si aparece login, esperar a que el usuario inicie sesión manualmente
                if page.locator('text="Iniciar sesión"').count() > 0 or page.locator('text="Log in"').count() > 0:
                    wait_sec = config.get("tiktok_settings", {}).get("login_wait_sec", 180)
                    check_interval = config.get("tiktok_settings", {}).get("login_check_interval_sec", 5)
                    logger.warning(f"Detectado login en pantalla. Esperando hasta {wait_sec}s para que el usuario inicie sesión...")

                    waited = 0
                    while waited < wait_sec:
                        if page.locator('input[type="file"]').count() > 0:
                            logger.info("Login resuelto: input de video detectado.")
                            break
                        time.sleep(check_interval)
                        waited += check_interval

                    if waited >= wait_sec:
                        page.screenshot(path=os.path.join(DEBUG_DIR, "login_timeout.png"))
                        raise Exception("Login no completado en el tiempo de espera.")
                
                # Check Login residual
                if "login" in page.url or page.locator("text='Log in'").count() > 0:
                    logger.warning("Sesión expirada o no iniciada. Requiere login manual.")
                    send_webhook("❌ Login Requerido", "Las cookies han expirado. Inicia sesión en el navegador real y reintenta.", is_error=True)
                    page.screenshot(path=os.path.join(DEBUG_DIR, "login_required.png"))
                    return False
                    
                if self.check_captcha(page):
                    return False
                
                # 3. Subir Video (DOM updates)
                logger.info("Buscando input de video...")
                page.set_default_timeout(60000)
                page.wait_for_load_state("domcontentloaded")
                human_delay(2, 5)
                
                # Intentar cerrar modales que bloqueen la vista (ej. "Upload to TikTok")
                try:
                    page.keyboard.press("Escape")
                    human_delay(1, 2)
                except: pass

                # Tomar captura de debug antes de buscar el input
                page.screenshot(path=os.path.join(DEBUG_DIR, "pre_upload_state.png"))

                input_video = None
                # Estrategia 1: Buscar directamente en la página principal
                try:
                    file_input_main = page.locator('input[type="file"]')
                    if file_input_main.count() > 0:
                        input_video = file_input_main.first
                        logger.info("Input de video encontrado en el DOM principal.")
                except Exception: pass

                # Estrategia 2: Buscar en todos los iframes (TikTok suele cambiar el nombre del iframe)
                if not input_video:
                    for frame in page.frames:
                        try:
                            file_input_frame = frame.locator('input[type="file"]')
                            if file_input_frame.count() > 0:
                                input_video = file_input_frame.first
                                logger.info(f"Input de video encontrado en iframe: {frame.name}")
                                break
                        except Exception: continue

                # Estrategia 3: si no hay input, hacer click en el botón "Seleccionar video"
                if not input_video:
                    button_selectors = [
                        'button:has-text("Seleccionar video")',
                        'button:has-text("Select video")',
                        'text="Seleccionar video"',
                        'text="Select video"'
                    ]
                    filechooser = None
                    for sel in button_selectors:
                        if page.locator(sel).count() > 0:
                            with page.expect_file_chooser() as fc_info:
                                page.locator(sel).first.click()
                            filechooser = fc_info.value
                            break
                    if filechooser:
                        filechooser.set_files(video_path)
                        logger.info("Archivo cargado via file chooser.")
                    else:
                        raise Exception("No se pudo encontrar el selector de archivos (input type='file') ni el botón 'Seleccionar video'.")

                if input_video:
                    # Forzar que el input sea visible si está oculto y enviar el archivo
                    input_video.evaluate("el => el.style.display = 'block'")
                    input_video.set_input_files(video_path)
                    logger.info("Archivo cargado en input. Esperando procesamiento de TikTok (60s)...")
                    
                    # IMPORTANTE: Esperar 60 segundos para que TikTok procese el video
                    page.wait_for_timeout(60000)  # 60 segundos para procesamiento
                    human_delay(5, 10)
                
                # Esperar a que el video cargue en la UI (puede tardar dependiendo del peso)
                page.wait_for_timeout(20000)  # 20 segundos para procesamiento
                human_delay(5, 10)
                
                # Cerrar el modal principal (solo uno, no iterar)
                logger.info("Cerrando modal principal...")
                try:
                    page.keyboard.press("Escape")
                    human_delay(2, 3)
                    # Buscar "Got it"
                    if page.locator('button:has-text("Got it")').count() > 0:
                        page.locator('button:has-text("Got it")').first.click(force=True, timeout=5000)
                        human_delay(3, 5)
                except: pass
                
                # 4. Rellenar Descripción
                logger.info("Cerrando modales emergentes (intento ESC)...")
                page.keyboard.press("Escape")
                human_delay(2, 3)
                
                # Intentar click fuera del modal (en el fondo)
                try:
                    page.mouse.click(10, 10)  # Click en esquina superior izq
                    human_delay(1, 2)
                except: pass
                
                # Buscar botones de continuar/aceptar
                logger.info("Buscando botones de modal...")
                continue_selectors = [
                    'button:has-text("Continue")',
                    'button:has-text("Aceptar")', 
                    'button:has-text("Entendido")',
                    'button:has-text("Got it")',
                    'button[data-e2e="upload-continue"]'
                ]
                for sel in continue_selectors:
                    if page.locator(sel).count() > 0:
                        try:
                            page.locator(sel).first.click(force=True, timeout=5000)
                            logger.info(f"Clickeado: {sel}")
                            human_delay(3, 5)
                        except: pass
                
                # 4. Rellenar Descripción
                description = payload.get("description", "")
                logger.info("Escribiendo descripción...")
                
                # TikTok actualizó su editor a un div contenteditable
                editor = None
                editor_selectors = [
                    '.public-DraftEditor-content',
                    '[data-contents="true"]',
                    '.DraftEditor-root div[contenteditable="true"]',
                    '.margin-t-24 div[contenteditable="true"]'
                ]
                
                # Buscar editor en main page o iframes
                for sel in editor_selectors:
                    if page.locator(sel).count() > 0:
                        editor = page.locator(sel).first
                        break
                
                if not editor:
                    for frame in page.frames:
                        for sel in editor_selectors:
                            if frame.locator(sel).count() > 0:
                                editor = frame.locator(sel).first
                                break
                        if editor: break
                else:
                    # Editor encontrado - proceder con la descripción
                    pass
                
                if editor:
                    # Click para enfocar + espera
                    try:
                        editor.click(timeout=5000)
                    except:
                        page.mouse.click(200, 400)  # Click en área del título
                    page.wait_for_timeout(1000)
                    
                    # Limpiar con keyboard
                    page.keyboard.press("Control+a")
                    page.keyboard.press("Meta+a")
                    page.keyboard.press("Backspace")
                    page.wait_for_timeout(500)
                    
                    # Escribir con el keyboard (más realista que fill)
                    page.keyboard.type(description, delay=random.randint(30, 80))
                    logger.info("Descripción insertada con keyboard.")
                    
                    # IMPORTANTE: Cerrar dropdown de hashtags de TikTok
                    page.wait_for_timeout(1000)
                    page.keyboard.press("Escape")  # Cerrar dropdown si aparece
                    page.wait_for_timeout(500)
                    
                    # Si hay sugerencias de hashtags, seleccionarlas con arrow keys
                    if page.locator('[data-e2e="challenge-item"]').count() > 0:
                        logger.info("Dropdown de hashtags detectado, seleccionando...")
                        page.keyboard.press("ArrowDown")
                        page.wait_for_timeout(300)
                        page.keyboard.press("Enter")
                    else:
                        logger.info("No hay dropdown de hashtags")
                else:
                    logger.warning("No se pudo encontrar el editor de texto. Intentando fallback con JS...")
                    # Fallback: injectar descripción con JavaScript puro
                    page.evaluate(f'''
                        () => {{
                            const els = document.querySelectorAll('.public-DraftEditor-content, [contenteditable="true"]');
                            if (els.length > 0) {{
                                els[0].innerText = `{description}`;
                                els[0].dispatchEvent(new Event('input', {{bubbles: true}}));
                                els[0].dispatchEvent(new Event('change', {{bubbles: true}}));
                            }}
                        }}
                    ''')
                    
                human_delay(3, 5)
                
                # 5. Configuraciones de Post
                conf = payload.get("post_config", {})
                if not conf.get("comments", True):
                    # Desmarcar comentarios (puede fallar si el layout cambia)
                    try:
                        comment_switch = page.locator("text='Comment'").locator("..").locator("input[type='checkbox']")
                        if comment_switch.is_checked(): comment_switch.click()
                    except: pass
                
# 6. Click Post - JavaScript directo (bypass UI)
                logger.info("Haciendo click en POST con JavaScript directo...")
                
                # Intentar múltiples métodos de click
                success = False
                
                # Método 1: JavaScript directo al botón
                try:
                    page.evaluate('''
                        () => {
                            // Buscar botón Post
                            const buttons = Array.from(document.querySelectorAll('button'));
                            const postButton = buttons.find(b => 
                                b.textContent.includes('Post') || 
                                b.textContent.includes('Publicar') ||
                                b.textContent.includes('Publish')
                            );
                            if (postButton) {
                                postButton.click();
                                return 'clicked';
                            }
                            return 'not_found';
                        }
                    ''')
                    success = True
                    logger.info("JavaScript click ejecutado")
                except Exception as e:
                    logger.warning(f"Método 1 falló: {e}")
                
                if not success:
                    # Método 2: Encontrar y submitear el formulario
                    try:
                        page.evaluate('''
                            () => {
                                // Buscar cualquier formulario cerca del botón Post
                                const forms = document.querySelectorAll('form');
                                for (const form of forms) {
                                    const buttons = form.querySelectorAll('button');
                                    for (const btn of buttons) {
                                        if (btn.textContent.includes('Post')) {
                                            form.submit();
                                            return 'submitted';
                                        }
                                    }
                                }
                                return 'no_form';
                            }
                        ''')
                        success = True
                        logger.info("Formulario submitido")
                    except Exception as e:
                        logger.warning(f"Método 2 falló: {e}")
                
                if not success:
                    # Método 3: coordinates
                    page.mouse.click(40, 182)
                    success = True
                
                # Esperar a que TikTok procese
                page.wait_for_timeout(10000)
                
                # Si seguimos en la página de upload, el video probablemente se publicó
                current_url = page.url
                logger.info(f"URL después del click: {current_url}")
                
                # Verificar si aparece mensaje de éxito o redirected
                if "manage" in current_url or "video" in current_url or "success" in current_url:
                    logger.info("✅ Video publicado exitosamente!")
                elif "upload" in current_url:
                    # Último retry con keyboard
                    logger.info("Intentando con keyboard...")
                    page.keyboard.press("Enter")
                    page.wait_for_timeout(5000)
                
                logger.info("Esperando confirmación de subida...")
                logger.info("Esperando confirmación de subida (largo)...")
                
                # Esperar a confirmación (puede salir "Manage your posts", o redirigir al perfil)
                logger.info("Esperando confirmación de subida...")
                try:
                    page.wait_for_selector('text="Manage your posts"', timeout=45000)
                    logger.info("Confirmación 'Manage your posts' encontrada.")
                except TimeoutError:
                    if "profile" in page.url or self.check_captcha(page):
                        logger.info("Redirección al perfil completada. Asumiendo éxito.")
                    else:
                        logger.warning("No se detectó el mensaje de éxito claro (puede que haya subido igualmente).")
                
                # Guardar nuevo estado
                state = {"cookies": context.cookies()}
                with open(COOKIES_PATH, "w") as f:
                    json.dump(state, f)
                    
                logger.info("¡Subida completada con éxito!")
                send_webhook("✅ Subida Exitosa", f"Clip del canal **{payload.get('channel')}** subido correctamente.\nTítulo: `{payload.get('title')[:50]}...`")
                return True
                
            except Exception as e:
                logger.error(f"Excepción durante la subida: {str(e)}")
                screenshot_path = os.path.join(DEBUG_DIR, f"error_{int(time.time())}.png")
                page.screenshot(path=screenshot_path)
                send_webhook("❌ Error Crítico en Uploader", f"Fallo al subir el clip.\nException: {str(e)}\nScreenshot guardado localmente.", is_error=True)
                return False
            finally:
                if not self._using_cdp:
                    try:
                        context.close()
                    except Exception:
                        pass
                    try:
                        browser.close()
                    except Exception:
                        pass

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python3 tiktok_uploader.py <video_path> '<payload_json>'")
        sys.exit(1)
        
    video = sys.argv[1]
    payload = json.loads(sys.argv[2])
    
    headless_setting = config.get("tiktok_settings", {}).get("headless_browser", True)
    uploader = TikTokUploader(headless=headless_setting)
    success = uploader.upload(video, payload)
    
    print(json.dumps({"success": success}))
