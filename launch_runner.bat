@echo off
cd /d "%~dp0"
python -X utf8 -u main_runner.py >> dry_run_output.log 2>&1
