@echo off
cd /d "%~dp0.."
call conda activate dbstudio
start "" http://localhost:8080
python server.py
