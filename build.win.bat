@echo off
setlocal enabledelayedexpansion

echo ==================================================
echo Plotune MQTT Extension Builder (Windows / onefile)
echo ==================================================

:: Activate virtual environment
if exist ".venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call .venv\Scripts\activate.bat
) else (
    echo Virtual environment not found. Creating .venv...
    python -m venv .venv
    call .venv\Scripts\activate.bat
)

:: Install requirements
echo Installing Python dependencies...
pip install --upgrade pip
if exist "requirements.txt" (
    pip install -r requirements.txt
)

:: Ensure PyInstaller is installed
pip show pyinstaller >nul 2>&1
if errorlevel 1 (
    echo PyInstaller not found. Installing...
    pip install pyinstaller
)

:: Variables
set APP_NAME=plotune_mqtt_extension
set ZIP_NAME=plotune-mqtt-extension-windows-x86_64.zip
set DIST_DIR=dist
set HISTORY_DIR=%DIST_DIR%\history

:: History folder
if not exist "%HISTORY_DIR%" mkdir "%HISTORY_DIR%"

:: Archive previous ZIP
if exist "%DIST_DIR%\%ZIP_NAME%" (
    for /f "tokens=1-6 delims=/:. " %%a in ("%date% %time%") do (
        set DATE_TAG=%%c%%a%%b_%%d%%e%%f
    )
    echo Moving existing ZIP to history: !DATE_TAG!
    move "%DIST_DIR%\%ZIP_NAME%" "%HISTORY_DIR%\%ZIP_NAME%_!DATE_TAG!"
)

:: Build EXE (ONEFILE)
echo Building executable...
pyinstaller --name %APP_NAME% ^
            --onefile ^
            --noconfirm ^
            --icon assets/logo.ico ^
            src\main.py

:: Copy plugin.json next to EXE
echo Copying plugin.json...
copy src\plugin.json "%DIST_DIR%\plugin.json" /Y

:: Patch plugin.json (UTF-8 no BOM)
echo Patching plugin.json cmd field (no BOM)...
python scripts\patch_plugin.py

:: Create ZIP
echo Creating ZIP archive...
cd %DIST_DIR%

powershell -NoProfile -Command ^
"Compress-Archive -Path '%APP_NAME%.exe','plugin.json' -DestinationPath '%ZIP_NAME%' -Force"

:: SHA256
certutil -hashfile %ZIP_NAME% SHA256 | findstr /v "hash" > %ZIP_NAME%.sha256

cd ..

echo ==================================================
echo Build completed successfully (Windows / onefile)
echo ==================================================
pause
