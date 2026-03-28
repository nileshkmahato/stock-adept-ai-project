import json
from datetime import datetime
import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_PATH = os.path.join(BASE_DIR, "nse_holidays.json")

today = datetime.now().strftime("%Y-%m-%d")

try:
    with open(JSON_PATH, "r") as f:
        holidays = json.load(f)
except FileNotFoundError:
    print(f"Error: Could not find {JSON_PATH}")
    sys.exit(1)

for holiday in holidays:
    if holiday["date"] == today:
        print(
            f"NSE Holiday Today\n"
            f"Date     : {holiday['date']}\n"
            f"Day       : {holiday['day']}\n"
            f"Festival : {holiday['festival']}"
        )
        sys.exit(1)  # Stop GitHub Action

print(f"Trading Day: {today} — proceeding with workflow")