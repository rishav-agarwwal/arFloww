import pycron
import subprocess
import time
from datetime import datetime

print("📆 Cron runner started...")

while True:
    now = datetime.now().strftime('%H:%M')

    try:
        if pycron.is_now('1 0 * * *'):  # 00:01 daily
            print(f"[{now}] Running: inputTrigger.py")
            result = subprocess.run(["python", "inputTrigger.py"], capture_output=True, text=True)

            # Check if inputTrigger.py succeeded before running outputTrigger.py
            if result.returncode == 0:
                print(f"[{now}] ✅ inputTrigger.py completed. Running: outputTrigger.py")
                subprocess.call(["python", "outputTrigger.py"])
            else:
                print(f"[{now}] ❌ inputTrigger.py failed. Skipping output generation.")
                with open("cron_errors.log", "a") as f:
                    f.write(f"[{datetime.now()}] inputTrigger.py failed:\n{result.stderr}\n")

        if pycron.is_now('0 23 * * *'):  # 23:00 daily
            print(f"[{now}] Running: inputTriggerII.py")
            subprocess.call(["python", "inputTriggerII.py"])

    except Exception as e:
        with open("cron_errors.log", "a") as f:
            f.write(f"[{datetime.now()}] ❌ Unexpected error: {str(e)}\n")

    time.sleep(60)
