@echo off
setlocal
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" run.py
) else (
    py run.py
)

if errorlevel 1 (
    echo.
    echo El arranque de Nexus Madrid ha fallado.
    pause
)
