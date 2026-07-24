@echo off
echo =========================================
echo   OpenClaw WhatsApp Server Launcher
echo =========================================
echo.

REM Check if cloudflared exists
if not exist "cloudflared.exe" (
    echo [ERROR] cloudflared.exe not found in this directory.
    echo Download it from: https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe
    echo Rename it to cloudflared.exe and place it in this folder.
    pause
    exit /b 1
)

REM Check if .env exists
if not exist ".env" (
    echo [ERROR] .env file not found. Copy .env.example to .env and fill in your values.
    pause
    exit /b 1
)

echo [1/2] Starting WhatsApp Baileys server on port 3001...
start "WhatsApp Server" cmd /k "node server.js"

echo Waiting 3 seconds for server to initialize...
timeout /t 3 > nul

echo [2/2] Starting Cloudflare Tunnel...
echo.
echo   WhatsApp server URL is permanently configured at:
echo   WHATSAPP_SERVER_URL = https://whatsapp.craftstore.co.ke
echo.
cloudflared.exe tunnel --protocol http2 --config config.yml run whatsapp-tunnel
