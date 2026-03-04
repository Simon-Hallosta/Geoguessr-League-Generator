@echo off
setlocal
cd /d "%~dp0.."
where py >nul 2>&1
if %errorlevel%==0 (
  py -3 desktop_app\app.py
) else (
  python desktop_app\app.py
)
endlocal
