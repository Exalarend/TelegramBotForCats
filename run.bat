@echo off
cd /d "%~dp0"
start /min "" ".venv\Scripts\python.exe" main.py --tray
