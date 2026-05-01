#!/usr/bin/env python3
"""
KickClipBot — tiktok_login.py
Herramienta dedicada para iniciar sesión en TikTok y guardar las cookies.
"""

import os
import json
import time
from playwright.sync_api import sync_playwright

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COOKIES_DIR = os.path.join(SCRIPT_DIR, "cookies")
os.makedirs(COOKIES_DIR, exist_ok=True)
COOKIES_PATH = os.path.join(COOKIES_DIR, "tiktok_state.json")

def login():
    print("\n=======================================================")
    print("🎬 INICIO DE SESIÓN MANUAL EN TIKTOK")
    print("=======================================================")
    print("Se abrirá una ventana del navegador.")
    print("1. Inicia sesión (puedes usar QR, Google, o tu teléfono).")
    print("2. Resuelve cualquier CAPTCHA que aparezca.")
    print("3. Vuelve a esta consola y presiona ENTER cuando hayas terminado.")
    print("=======================================================\n")
    
    with sync_playwright() as p:
        # Abrimos el navegador VISIBLE (headless=False)
        browser = p.chromium.launch(headless=False, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(viewport={"width": 1280, "height": 720})
        page = context.new_page()
        
        # Cargar cookies previas si existen (para no empezar de cero si ya había algo)
        if os.path.exists(COOKIES_PATH):
            try:
                with open(COOKIES_PATH, "r") as f:
                    state = json.load(f)
                context.add_cookies(state.get("cookies", []))
            except Exception:
                pass
                
        # Navegar al login
        page.goto("https://www.tiktok.com/login", timeout=60000)
        
        # Esperamos a que el usuario confirme en la terminal
        input("\n👉 Presiona ENTER aquí en la consola SOLAMENTE cuando ya hayas iniciado sesión y veas tu perfil de TikTok... ")
        
        # Guardar las cookies
        state = {"cookies": context.cookies()}
        with open(COOKIES_PATH, "w") as f:
            json.dump(state, f)
            
        print(f"\n✅ ¡Excelente! Sesión guardada exitosamente en {COOKIES_PATH}")
        print("Ya puedes cerrar el navegador. El bot ahora subirá los videos de forma invisible.")
        
        context.close()
        browser.close()

if __name__ == "__main__":
    login()