@echo off
setlocal
cd /d "%~dp0.."

set "EXE_PATH="
for /f "delims=" %%F in ('dir /b /a:-d /o-d "dist\GeoLeagueBuilder*.exe" 2^>nul') do (
  set "EXE_PATH=dist\%%F"
  goto :run_exe
)

:run_exe
if defined EXE_PATH (
  echo [INFO] Startar %EXE_PATH%
  "%EXE_PATH%"
  set "EXIT_CODE=%ERRORLEVEL%"
  if not "%EXIT_CODE%"=="0" (
    echo.
    echo [ERROR] %EXE_PATH% avslutades med felkod %EXIT_CODE%.
    echo Prova att starta EXE-filen direkt fran dist-mappen.
    pause
  )
  exit /b %EXIT_CODE%
)

echo [INFO] Hittade ingen GeoLeagueBuilder*.exe i dist\. Forsoker starta Python-varianten...
where py >nul 2>&1
if %ERRORLEVEL%==0 (
  py -3 desktop_app\app.py
) else (
  where python >nul 2>&1
  if %ERRORLEVEL%==0 (
    python desktop_app\app.py
  ) else (
    echo [ERROR] Varken GeoLeagueBuilder.exe eller Python hittades.
    echo Installera Python 3 eller lagg EXE i dist\\.
    pause
    exit /b 1
  )
)

set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
  echo.
  echo [ERROR] Python-korning misslyckades med felkod %EXIT_CODE%.
  pause
)

exit /b %EXIT_CODE%
