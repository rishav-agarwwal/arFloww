import pandas as pd
import psycopg2
import json
import os
from datetime import datetime

# ---------- CONFIG ----------
BASE_PATH = "D:\\Downloads\\AR Flow"
CONFIG_PATH = os.path.join(BASE_PATH, "config", "dbconfig.json")
TODB_PATH = os.path.join(BASE_PATH, "ToDB")
TODAY = datetime.today().strftime("%Y%m%d")

# ---------- DB CONNECTION ----------
def load_db_config(path=CONFIG_PATH):
    with open(path, "r") as f:
        return json.load(f)

def connect_db():
    return psycopg2.connect(**load_db_config())

# ---------- CSV TO DB UPLOAD ----------
def upload_csv_to_db(csv_path, table_name):
    config = load_db_config()
    conn = psycopg2.connect(**config)
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
    print(f"✅ Synced {affected} invoices as closed in ar_invoices table")

# ---------- MAIN ----------
if __name__ == "__main__":
    payments_file = os.path.join(TODB_PATH, f"payments_{TODAY}.csv")

    if os.path.exists(payments_file):
        upload_csv_to_db(payments_file, "payments")
        update_invoice_status()
    else:
        print(f"❌ File not found: {payments_file}")
