@echo off
cd /d "D:\Downloads\AR Flow"
echo [INFO] Starting triggerCron.py with self-healing loop...

:loop
echo [%date% %time%] 🚀 Launching triggerCron.py
python triggerCron.py

echo [%date% %time%] ❌ triggerCron.py exited or crashed. Restarting in 5 seconds...
timeout /t 5 >nul
goto loop
