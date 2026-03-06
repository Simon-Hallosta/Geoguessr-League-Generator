@echo off
setlocal EnableDelayedExpansion

REM Canonical Windows build for GeoLeagueBuilder.exe
REM Run from project root or via scripts\build_exe_windows.bat

set "ROOT_DIR=%~dp0.."
pushd "%ROOT_DIR%" >nul

if not exist ".venv-win\Scripts\python.exe" (
  echo [ERROR] Missing .venv-win\Scripts\python.exe
  echo Create/install the Windows venv dependencies first.
  popd >nul
  exit /b 1
)

".venv-win\Scripts\python.exe" -c "import matplotlib, matplotlib.pyplot" >nul 2>&1
if errorlevel 1 (
  echo [INFO] matplotlib saknas i .venv-win. Installerar...
  ".venv-win\Scripts\python.exe" -m pip install --disable-pip-version-check matplotlib
  if errorlevel 1 (
    echo [ERROR] Kunde inte installera matplotlib.
    popd >nul
    exit /b 1
  )
)

set "WORKPATH=%LOCALAPPDATA%\Temp\GeoLeagueBuilder_build"
set "EXE_NAME=GeoLeagueBuilder"

if exist "dist\GeoLeagueBuilder.exe" (
  copy /b "dist\GeoLeagueBuilder.exe"+,, "dist\GeoLeagueBuilder.exe" >nul 2>&1
  if errorlevel 1 (
    for /L %%N in (1,1,200) do (
      if not exist "dist\GeoLeagueBuilder_%%N.exe" (
        set "EXE_NAME=GeoLeagueBuilder_%%N"
        goto :name_selected
      )
    )
  )
)
:name_selected

".venv-win\Scripts\python.exe" -m PyInstaller ^
  --noconfirm ^
  --onefile ^
  --windowed ^
  --workpath "%WORKPATH%" ^
  --icon "desktop_app\assets\geoleague.ico" ^
  --name "!EXE_NAME!" ^
  --hidden-import matplotlib ^
  --hidden-import matplotlib.pyplot ^
  --exclude-module scipy ^
  --exclude-module PyQt5 ^
  --exclude-module PySide6 ^
  --exclude-module IPython ^
  --exclude-module jupyter ^
  --exclude-module notebook ^
  --exclude-module black ^
  "desktop_app\app.py"

set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
  echo [ERROR] Build failed with exit code %EXIT_CODE%.
  popd >nul
  exit /b %EXIT_CODE%
)

echo [OK] Build complete: dist\!EXE_NAME!.exe
popd >nul
exit /b 0
