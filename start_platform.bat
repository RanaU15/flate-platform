@echo off
title Facebook Scraper Platform Launcher
echo ========================================================
echo Starting Facebook Scraper Platform Services
echo ========================================================

echo 1. Starting Isolated PocketBase (Port 8092)...
start "PocketBase :8092" /min cmd /c "cd /d D:\facebook-scraper-platform\pocketbase && pocketbase.exe serve --http=127.0.0.1:8092 --dir=D:\facebook-scraper-platform\pocketbase\pb_data"

timeout /t 3 /nobreak >nul

echo 2. Starting Backend API Server (Port 4000)...
start "Backend API :4000" /min cmd /c "cd /d D:\facebook-scraper-platform\server && node server.js"

timeout /t 2 /nobreak >nul

echo 3. Starting Astro Frontend Console (Port 3000)...
start "Frontend Console :3000" /min cmd /c "cd /d D:\facebook-scraper-platform\frontend && pnpm dev"

echo.
echo ========================================================
echo Platform is online!
echo Console Dashboard: http://localhost:3000
echo PocketBase Admin:  http://127.0.0.1:8092/_/
echo Backend REST API:  http://127.0.0.1:4000
echo ========================================================
pause

