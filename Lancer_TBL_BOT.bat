@echo off
title TBlaugrana BOT

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

:: --- Configuration Telegram (usage LOCAL uniquement) ---------------------
:: Le token n'est plus ecrit en dur dans server.py (pour eviter qu'il ne
:: fuite si le code est publie sur GitHub). Remplissez les 2 lignes
:: ci-dessous avec vos propres valeurs pour activer Telegram en local.
:: Sur Railway, definissez plutot ces memes valeurs dans l'onglet Variables.
set TELEGRAM_ON=false
set TELEGRAM_TOKEN=
set TELEGRAM_CHAT_IDS=

:: Si argument FERMER, tuer le serveur Python via l'API shutdown
if /i "%1"=="FERMER" goto :fermer

:: Demarrer le serveur Python completement invisible (pas de fenetre)
start /b pythonw server.py 2>nul
if errorlevel 1 (
    start /b python server.py
)

:: Attendre que le serveur soit pret
timeout /t 2 /nobreak >nul

:: Cherche Chrome puis Edge pour ouvrir en mode app (vraie fenetre sans navigateur)
set CHROME_PATH=
for %%P in ("%ProgramFiles%\Google\Chrome\Application\chrome.exe" "%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" "%LocalAppData%\Google\Chrome\Application\chrome.exe") do (
    if exist %%P set CHROME_PATH=%%~P
)

set EDGE_PATH=
for %%P in ("%ProgramFiles%\Microsoft\Edge\Application\msedge.exe" "%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe") do (
    if exist %%P set EDGE_PATH=%%~P
)

if defined CHROME_PATH (
    start "" "%CHROME_PATH%" --app=http://localhost:8765 --window-size=440,820 --window-position=100,50 --no-first-run --no-default-browser-check
    exit
)

if defined EDGE_PATH (
    start "" "%EDGE_PATH%" --app=http://localhost:8765 --window-size=440,820 --window-position=100,50 --no-first-run
    exit
)

:: Fallback navigateur par defaut
start "" "http://localhost:8765"
exit

:fermer
:: Envoyer la commande shutdown au serveur Python via l'API (methode propre)
curl -s -X POST http://localhost:8765/api/shutdown >nul 2>&1
if not errorlevel 1 goto :fermer_ok

:: Fallback : utiliser le fichier PID ecrit par Python au demarrage
if exist "%SCRIPT_DIR%tblbot.pid" (
    set /p TBL_PID=<"%SCRIPT_DIR%tblbot.pid"
    if defined TBL_PID (
        taskkill /f /pid %TBL_PID% >nul 2>&1
        del /f /q "%SCRIPT_DIR%tblbot.pid" >nul 2>&1
    )
) else (
    :: Dernier recours : tuer pythonw uniquement (pas python.exe generique)
    taskkill /f /im pythonw.exe >nul 2>&1
)

:fermer_ok
echo Serveur TBlaugrana arrete.
timeout /t 1 /nobreak >nul
exit
