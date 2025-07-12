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

# Connect to DB and load tables
def fetch_data():
    cfg = load_db_config()
    conn = psycopg2.connect(**cfg)
    ar = pd.read_sql("SELECT * FROM ar_invoices", conn)
    pay = pd.read_sql("SELECT * FROM payments", conn)
    conn.close()
    return ar, pay

# Build customer summary
def create_customer_summary(ar, pay):
    open_amt = ar[ar.status == 1].groupby("customer_id")["amountall"].sum().rename("total_open_amount")
    paid_amt = pay.groupby("customer_id")["amountall"].sum().rename("total_paid_amount")

    ar["past_due_days"] = (ar["paid_date"] - ar["due_date"]).apply(lambda x: x.days)

    avg_days = ar[ar.status == 1].groupby("customer_id")["past_due_days"].mean().round(2).rename("avg_past_due_days")
    late_pct = (pay[ar["past_due_days"] > 0]
                  .groupby("customer_id")["invoice_no"]
                  .count()
                  .div(pay.groupby("customer_id")["invoice_no"].count())
                  .mul(100)
                  .round(2)
                  .rename("late_payment_percentage"))

    summary = pd.concat([open_amt, paid_amt, avg_days, late_pct], axis=1).fillna(0).reset_index()

    def tag_behavior(days):
        if days <= -5: return "Early Payer"
        elif -5 < days <= 10: return "On-Time"
        elif days > 10: return "Late Payer"
        else: return "Unknown"

    summary["payment_behavior_tag"] = summary["avg_past_due_days"].apply(tag_behavior)
    return summary

# Export to CSV
def export_summary(summary):
    path = os.path.join(OUTPUT_DIR, f"customer_summary.csv")
    summary.to_csv(path, index=False)
    print(f"✅ Customer summary exported: {path}")

if __name__ == "__main__":
    ar, pay = fetch_data()
    summary = create_customer_summary(ar, pay)
    export_summary(summary)
