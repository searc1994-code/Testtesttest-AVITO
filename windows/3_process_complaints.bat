@echo off
cd /d %~dp0..
python complaint_worker.py --max-items 2
pause
