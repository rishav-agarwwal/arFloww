import pandas as pd
import psycopg2
import json
import random
import os
from datetime import datetime

# Constants
TODAY = datetime.today().date()
DATE_SUFFIX = TODAY.strftime("%Y%m%d")
OUTPUT_DIR = "D:\\Downloads\\AR Flow\\ToDB"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# DB Config Loader
def load_db_config(path="D:\\Downloads\\AR Flow\\config\\dbconfig.json"):
    with open(path, "r") as f:
        return json.load(f)

# Connect and fetch open AR records
def fetch_open_invoices():
    config = load_db_config()
    conn = psycopg2.connect(**config)
    query = "SELECT * FROM ar_invoices WHERE status = 1"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Simulate closures (random 20%–50% of open invoices)
def simulate_closing(open_df):
    lower = len(open_df) // 5
    upper = len(open_df) // 2
    closing_count = random.randint(lower, upper)
    closed_df = open_df.sample(n=closing_count).copy()
    closed_df["status"] = 0  # update status only
    closed_df["paid_date"] = TODAY  # add paid_date column
    return closed_df

# Write updated snapshot to CSV
def export_snapshot(closed_df):
    snapshot_path = os.path.join(OUTPUT_DIR, f"payments_{DATE_SUFFIX}.csv")
    closed_df.to_csv(snapshot_path, index=False)
    print(f"✅ Snapshot saved: {snapshot_path}")

if __name__ == "__main__":
    open_df = fetch_open_invoices()
    closed_df = simulate_closing(open_df)
    export_snapshot(closed_df)
