import pandas as pd
import psycopg2
import json
import os
from datetime import datetime

# Import the centralized database connection function
from db_connector import connect_db

# ---------- CONFIG ----------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(SCRIPT_DIR)
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config", "dbconfig.json")
TODB_PATH = os.path.join(BASE_PATH, "ToDB")
TODAY = datetime.today().strftime("%Y%m%d")

# ---------- CSV TO DB UPLOAD ----------
def upload_csv_to_db(csv_path, table_name):
    conn = connect_db()
    if conn is None:
        print("❌ Upload failed: Could not connect to database.")
        return

    cur = conn.cursor()

    df = pd.read_csv(csv_path, keep_default_na=False, na_values=[])
    df = df.fillna("NA")
    df = df.astype(str)

    for _, row in df.iterrows():
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        update_stmt = ', '.join([f"{col}=EXCLUDED.{col}" for col in df.columns if col != 'invoice_no'])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT (invoice_no) DO NOTHING;"
        cur.execute(sql, tuple(row))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Uploaded {len(df)} records from {csv_path} → {table_name} table")

# ---------- SYNC INVOICE STATUS ----------
def update_invoice_status():
    conn = connect_db()
    if conn is None:
        print("❌ Update failed: Could not connect to database.")
        return

    cur = conn.cursor()
    query = """
        UPDATE ar_invoices
        SET status = 0
        WHERE invoice_no IN (
            SELECT invoice_no FROM payments
        );
    """
    cur.execute(query)
    affected = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Synced {affected} records from payments table to ar_invoices.")

# ---------- MAIN EXECUTION ----------
if __name__ == "__main__":
    invoice_path = os.path.join(TODB_PATH, f"ar_invoices_{TODAY}.csv")
    upload_csv_to_db(invoice_path, "ar_invoices")

    payment_path = os.path.join(TODB_PATH, f"payments_{TODAY}.csv")
    upload_csv_to_db(payment_path, "payments")
    update_invoice_status()