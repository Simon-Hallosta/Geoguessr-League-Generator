@echo off
setlocal
cd /d "%~dp0.."

set "EXE_PATH=dist\GeoLeagueBuilder.exe"

if exist "%EXE_PATH%" (
  "%EXE_PATH%"
  set "EXIT_CODE=%ERRORLEVEL%"
  if not "%EXIT_CODE%"=="0" (
    echo.
    echo [ERROR] GeoLeagueBuilder.exe avslutades med felkod %EXIT_CODE%.
    echo Prova att starta EXE-filen direkt fran dist-mappen.
    pause
  )
  exit /b %EXIT_CODE%
)

echo [INFO] Hittade inte %EXE_PATH%. Forsoker starta Python-varianten...
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
