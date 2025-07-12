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

# Fetch required data
def fetch_data():
    cfg = load_db_config()
    conn = psycopg2.connect(**cfg)
    ar = pd.read_sql("SELECT * FROM ar_invoices WHERE status = 1", conn)
    customers = pd.read_sql("SELECT customer_id, region FROM customers", conn)
    conn.close()
    return ar, customers

# Merge and summarize region exposure
def compute_region_exposure(ar, customers):
    merged = ar.merge(customers, on="customer_id", how="left")
    summary = merged.groupby("region").agg(
        total_open_amount=("amountall", "sum"),
        total_customers=("customer_id", pd.Series.nunique)
    ).reset_index()
    return summary

# Export to CSV
def export_summary(df):
    path = os.path.join(OUTPUT_DIR, f"region_exposure.csv")
    df.to_csv(path, index=False)
    print(f"✅ Region exposure saved: {path}")

if __name__ == "__main__":
    ar, customers = fetch_data()
    summary = compute_region_exposure(ar, customers)
    export_summary(summary)
