@echo off
title PMU Bot - Serveur local
cd /d "%~dp0"

echo Demarrage du serveur local (proxy anti-CORS)...
start "Serveur PMU Bot" cmd /k python server.py

echo Attente du demarrage du serveur...
timeout /t 2 /nobreak >nul

set "FIREFOX1=C:\Program Files\Mozilla Firefox\firefox.exe"
set "FIREFOX2=C:\Program Files (x86)\Mozilla Firefox\firefox.exe"

if exist "%FIREFOX1%" (
    start "" "%FIREFOX1%" "http://127.0.0.1:8000/pmu_bot.html"
) else if exist "%FIREFOX2%" (
    start "" "%FIREFOX2%" "http://127.0.0.1:8000/pmu_bot.html"
) else (
    echo Firefox introuvable, ouverture avec le navigateur par defaut...
    start "" "http://127.0.0.1:8000/pmu_bot.html"
)
