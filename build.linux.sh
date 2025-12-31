#!/bin/bash
set -e

echo "=================================================="
echo "Plotune MQTT Extension Builder (Linux)"
echo "=================================================="

APP_NAME="plotune_mqtt_extension"
ARCHIVE_NAME="plotune-mqtt-extension-linux-x86_64.tar.gz"
DIST_DIR="dist"
HISTORY_DIR="$DIST_DIR/history"
VENV_DIR="..venv"

# --------------------------------------------------
# Virtual environment
# --------------------------------------------------
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment: $VENV_DIR"
    python3 -m venv "$VENV_DIR"
    "$VENV_DIR/bin/python" -m ensurepip --upgrade
fi

echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# --------------------------------------------------
# Dependencies
# --------------------------------------------------
echo "Installing Python dependencies..."
pip install --upgrade pip
[ -f requirements.txt ] && pip install -r requirements.txt

python - << 'EOF'
try:
    import PyInstaller
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
EOF

# --------------------------------------------------
# Prepare history
# --------------------------------------------------
mkdir -p "$HISTORY_DIR"

if [ -f "$DIST_DIR/$ARCHIVE_NAME" ]; then
    TS=$(date +"%Y%m%d_%H%M%S")
    echo "Archiving previous build: $TS"
    mv "$DIST_DIR/$ARCHIVE_NAME" "$HISTORY_DIR/${ARCHIVE_NAME}_$TS"
fi

# --------------------------------------------------
# Build executable
# --------------------------------------------------
echo "Building executable..."
pyinstaller \
    --name "$APP_NAME" \
    --onedir \
    --noconfirm \
    src/main.py

# --------------------------------------------------
# Mark binary as executable
# --------------------------------------------------
echo "Setting executable permission..."
chmod +x "$DIST_DIR/$APP_NAME/$APP_NAME"

# --------------------------------------------------
# Copy & patch plugin.json
# --------------------------------------------------
echo "Copying plugin.json..."
cp src/plugin.json "$DIST_DIR/$APP_NAME/plugin.json"

echo "Patching plugin.json for Linux executable..."
python - << 'EOF'
import json
from pathlib import Path

p = Path("dist/plotune_mqtt_extension/plugin.json")

data = json.loads(p.read_text(encoding="utf-8"))
data["cmd"] = ["./plotune_mqtt_extension"]

p.write_text(
    json.dumps(data, indent=4, ensure_ascii=False),
    encoding="utf-8"
)
EOF

# --------------------------------------------------
# Package
# --------------------------------------------------
echo "Creating tar.gz archive..."
cd "$DIST_DIR"
tar -czf "$ARCHIVE_NAME" "$APP_NAME"/*
sha256sum "$ARCHIVE_NAME" > "$ARCHIVE_NAME.sha256"
cd ..

echo "=================================================="
echo "Linux build completed successfully"
echo "=================================================="
