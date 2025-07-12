import pandas as pd
import psycopg2
import json
import os
from datetime import datetime

# Load DB config
def load_db_config(path="D:\\Downloads\\AR Flow\\config\\dbconfig.json"):
    with open(path, "r") as f:
        return json.load(f)

# Upload CSV to PostgreSQL
def upload_csv_to_db(csv_path, table_name="ar_invoices"):
    config = load_db_config()
    conn = psycopg2.connect(**config)
    cur = conn.cursor()

    df = pd.read_csv(csv_path)

    for _, row in df.iterrows():
        cur.execute(f"""
            INSERT INTO {table_name} (invoice_no, customer_id, amountall, create_date, due_date, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (invoice_no) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                amountall = EXCLUDED.amountall,
                create_date = EXCLUDED.create_date,
                due_date = EXCLUDED.due_date,
                status = EXCLUDED.status;
        """, tuple(row))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Uploaded {len(df)} records from {csv_path} → {table_name} table")

if __name__ == "__main__":
    today = datetime.today().strftime("%Y%m%d")
    filename = f"D:\\Downloads\\AR Flow\\ToDB\\ar_invoices_{today}.csv"

    if os.path.exists(filename):
        upload_csv_to_db(filename)
    else:
        print(f"❌ File not found: {filename}")
