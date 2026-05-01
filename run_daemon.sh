#!/bin/bash
#
# KickClipBot — AUTO-DAEMON RUNNER
# ================================
# Inicia el daemon de automatización
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Verificar dependencias
echo "=== KickClipBot Auto-Daemon ==="

if ! python3 -c "import clip_monitor" 2>/dev/null; then
    echo "Error: Faltan dependencias"
    exit 1
fi

# Asegurar directorios
mkdir -p logs clips processed uploaded data temp cookies

# Iniciar daemon
exec python3 auto_daemon.py "$@"