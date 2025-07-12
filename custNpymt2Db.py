import pandas as pd
import psycopg2
import json
import os
from datetime import datetime

# Load DB config
def load_db_config(path="D:\\Downloads\\AR Flow\\config\\dbconfig.json"):
    with open(path, "r") as f:
        return json.load(f)

# Upload CSV to specified table
def upload_csv_to_db(csv_path, table_name):
    config = load_db_config()
    conn = psycopg2.connect(**config)
    cur = conn.cursor()

    df = pd.read_csv(csv_path, keep_default_na=False, na_values=[])
    df = df.fillna("NA")  # Just in case
    df = df.astype(str)   # Force all values to string before upload


    if table_name == "customers":
        # Insert customers, update all columns on conflict
        for _, row in df.iterrows():
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            update_stmt = ', '.join([f"{col}=EXCLUDED.{col}" for col in df.columns if col != 'customer_id'])
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT (customer_id) DO UPDATE SET {update_stmt};"
            cur.execute(sql, tuple(row))
    elif table_name == "payments":
        # Insert payments, update all columns on conflict
        for _, row in df.iterrows():
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            update_stmt = ', '.join([f"{col}=EXCLUDED.{col}" for col in df.columns if col != 'invoice_no'])
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT (invoice_no) DO UPDATE SET {update_stmt};"
            cur.execute(sql, tuple(row))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Uploaded {len(df)} records from {csv_path} → {table_name} table")

if __name__ == "__main__":
    today = datetime.today().strftime("%Y%m%d")
    base_path = "D:\\Downloads\\AR Flow\\ToDB"

    files_tables = {
        f"payments_{today}.csv": "payments",
        f"customers_{today}.csv": "customers"
    }

    for file, table in files_tables.items():
        full_path = os.path.join(base_path, file)
        if os.path.exists(full_path):
            upload_csv_to_db(full_path, table)
        else:
            print(f"❌ File not found: {full_path}")
