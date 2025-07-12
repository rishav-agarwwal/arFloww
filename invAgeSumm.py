import pandas as pd
import psycopg2
import json
import os
from datetime import datetime

# Constants
TODAY = datetime.today().date()
DATE_SUFFIX = TODAY.strftime("%Y%m%d")
OUTPUT_DIR = "D:\\Downloads\\AR Flow\\Output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load DB config
def load_db_config(path="D:\\Downloads\\AR Flow\\config\\dbconfig.json"):
    with open(path, "r") as f:
        return json.load(f)

# Fetch AR data
def fetch_ar_data():
    cfg = load_db_config()
    conn = psycopg2.connect(**cfg)
    ar = pd.read_sql("SELECT * FROM ar_invoices WHERE status = 1", conn)
    conn.close()
    return ar

# Assign aging buckets
def classify_aging(df):
    df["days_overdue"] = (TODAY - df["due_date"]).apply(lambda x: x.days)

    def bucket(days):
        if days < 0:
            return "Not Due"
        elif days == 0:
            return "Due Today"
        elif days <= 30:
            return "0-30"
        elif days <= 60:
            return "31-60"
        elif days <= 90:
            return "61-90"
        else:
            return "90+"

    df["aging_bucket"] = df["days_overdue"].apply(bucket)
    return df

# Summarize aging buckets
def summarize_aging(df):
    summary = df.groupby("aging_bucket").agg(
        total_open_invoices=("invoice_no", "count"),
        total_amount=("amountall", "sum")
    ).reset_index()
    return summary

# Export

def export_summary(df):
    path = os.path.join(OUTPUT_DIR, f"invoice_aging_summary.csv")
    df.to_csv(path, index=False)
    print(f"✅ Aging summary saved: {path}")

if __name__ == "__main__":
    ar = fetch_ar_data()
    aged = classify_aging(ar)
    summary = summarize_aging(aged)
    export_summary(summary)