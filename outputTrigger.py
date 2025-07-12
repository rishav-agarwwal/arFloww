import pandas as pd
import psycopg2
import json
import os
from datetime import datetime

# ---------- CONFIG ----------
TODAY = datetime.today().date()
DATE_SUFFIX = TODAY.strftime("%Y%m%d")
OUTPUT_DIR = "D:\\Downloads\\AR Flow\\Output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------- DB CONNECTION ----------
def load_db_config(path="D:\\Downloads\\AR Flow\\config\\dbconfig.json"):
    with open(path, "r") as f:
        return json.load(f)

def connect_db():
    return psycopg2.connect(**load_db_config())

# ---------- REPORT 1: Customer Summary ----------
def create_customer_summary():
    conn = connect_db()
    ar = pd.read_sql("SELECT * FROM ar_invoices", conn)
    pay = pd.read_sql("SELECT * FROM payments", conn)
    conn.close()

    open_amt = ar[ar.status == 1].groupby("customer_id")["amountall"].sum().rename("total_open_amount")
    paid_amt = pay.groupby("customer_id")["amountall"].sum().rename("total_paid_amount")

    ar["past_due_days"] = (ar["paid_date"] - ar["due_date"]).apply(lambda x: x.days if pd.notnull(x) else 0)
    avg_days = ar[ar.status == 1].groupby("customer_id")["past_due_days"].mean().round(2).rename("avg_past_due_days")

    late = ar[(ar.status == 0) & (ar.past_due_days > 0)]
    late_pct = (late.groupby("customer_id")["invoice_no"].count()
                .div(ar[ar.status == 0].groupby("customer_id")["invoice_no"].count())
                .mul(100).round(2).rename("late_payment_percentage"))

    summary = pd.concat([open_amt, paid_amt, avg_days, late_pct], axis=1).fillna(0).reset_index()

    def tag_behavior(days):
        if days <= -5: return "Early Payer"
        elif -5 < days <= 10: return "On-Time"
        elif days > 10: return "Late Payer"
        else: return "Unknown"

    summary["payment_behavior_tag"] = summary["avg_past_due_days"].apply(tag_behavior)
    path = os.path.join(OUTPUT_DIR, "customer_summary.csv")
    summary.to_csv(path, index=False)
    print(f"✅ Customer summary exported → {path}")

# ---------- REPORT 2: Invoice Aging Summary ----------
def generate_invoice_aging_summary():
    conn = connect_db()
    ar = pd.read_sql("SELECT * FROM ar_invoices WHERE status = 1", conn)
    conn.close()

    ar["days_overdue"] = (TODAY - ar["due_date"]).apply(lambda x: x.days)

    def bucket(days):
        if days < 0: return "Not Due"
        elif days == 0: return "Due Today"
        elif days <= 30: return "0-30"
        elif days <= 60: return "31-60"
        elif days <= 90: return "61-90"
        else: return "90+"

    ar["aging_bucket"] = ar["days_overdue"].apply(bucket)

    summary = ar.groupby("aging_bucket").agg(
        total_open_invoices=("invoice_no", "count"),
        total_amount=("amountall", "sum")
    ).reset_index()

    path = os.path.join(OUTPUT_DIR, "invoice_aging_summary.csv")
    summary.to_csv(path, index=False)
    print(f"✅ Invoice aging summary exported → {path}")

# ---------- REPORT 3: Region Exposure ----------
def generate_region_exposure():
    conn = connect_db()
    ar = pd.read_sql("SELECT * FROM ar_invoices WHERE status = 1", conn)
    customers = pd.read_sql("SELECT customer_id, region FROM customers", conn)
    conn.close()

    merged = ar.merge(customers, on="customer_id", how="left")
    summary = merged.groupby("region").agg(
        total_open_amount=("amountall", "sum"),
        total_customers=("customer_id", pd.Series.nunique)
    ).reset_index()

    path = os.path.join(OUTPUT_DIR, "region_exposure.csv")
    summary.to_csv(path, index=False)
    print(f"✅ Region exposure summary exported → {path}")

# ---------- MAIN ----------
if __name__ == "__main__":
    create_customer_summary()
    generate_invoice_aging_summary()
    generate_region_exposure()