import pandas as pd
import psycopg2
import json
import os
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta

# ---------- CONFIG ----------
TODAY = datetime.today().date()
DATE_SUFFIX = TODAY.strftime("%Y%m%d")
BASE_PATH = "D:\\Downloads\\AR Flow"
TODB = os.path.join(BASE_PATH, "ToDB")
CONFIG_PATH = os.path.join(BASE_PATH, "config", "dbconfig.json")

os.makedirs(TODB, exist_ok=True)

fake = Faker()

# ---------- DB UTIL ----------
def load_db_config(path=CONFIG_PATH):
    with open(path, "r") as f:
        return json.load(f)

def connect_db():
    return psycopg2.connect(**load_db_config())

# ---------- 1. GENERATE AR INVOICES ----------
def generate_ar_invoices():
    num_records = random.randint(100, 2500)
    data = []
    for i in range(num_records):
        invoice_no = f"INV{str(i+1).zfill(7)}"
        customer_id = f"CUS{str(random.randint(1, 99999)).zfill(5)}"
        amount = round(random.uniform(100, 999999), 2)
        create_date = TODAY
        due_date = create_date + timedelta(days=random.choice([30, 60]))
        status = 1

        data.append({
            "invoice_no": invoice_no,
            "customer_id": customer_id,
            "amountall": amount,
            "create_date": create_date,
            "due_date": due_date,
            "status": status
        })

    df = pd.DataFrame(data)
    path = os.path.join(TODB, f"ar_invoices_{DATE_SUFFIX}.csv")
    df.to_csv(path, index=False)
    print(f"✅ AR Invoices generated → {path}")
    return df

# ---------- 2. UPLOAD TO DB ----------
def upload_to_postgres(df, table):
    conn = connect_db()
    cur = conn.cursor()
    for _, row in df.iterrows():
        if table == "ar_invoices":
            # Do nothing on conflict
            cur.execute(f"""
                INSERT INTO {table} ({', '.join(df.columns)})
                VALUES ({', '.join(['%s'] * len(df.columns))})
                ON CONFLICT ({df.columns[0]}) DO NOTHING;
            """, tuple(row))
        elif table == "customers":
            # Overwrite on conflict
            cur.execute(f"""
                INSERT INTO {table} ({', '.join(df.columns)})
                VALUES ({', '.join(['%s'] * len(df.columns))})
                ON CONFLICT ({df.columns[0]}) DO UPDATE SET
                {', '.join([f'{col} = EXCLUDED.{col}' for col in df.columns[1:]])};
            """, tuple(row))
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Uploaded {len(df)} records to {table} table")

# ---------- 3. SIMULATE PAYMENTS ----------
def simulate_payments():
    conn = connect_db()
    df = pd.read_sql("SELECT * FROM ar_invoices WHERE status = 1", conn)
    conn.close()
    sample_n = random.randint(len(df)//5, len(df)//2)
    closed_df = df.sample(n=sample_n).copy()
    closed_df["status"] = 0
    closed_df["paid_date"] = TODAY

    path = os.path.join(TODB, f"payments_{DATE_SUFFIX}.csv")
    closed_df.to_csv(path, index=False)
    print(f"✅ Payments simulated → {path}")
    return closed_df

# ---------- 4. GENERATE CUSTOMERS ----------
def generate_customers():
    conn = connect_db()
    df = pd.read_sql("SELECT DISTINCT customer_id FROM ar_invoices", conn)
    conn.close()
    ids = df.customer_id.tolist()

    industries = ["Retail", "Technology", "Manufacturing", "Finance", "Healthcare"]
    regions = ["NA", "EU", "APAC", "LATAM"]

    customers = []
    for cid in ids:
        customers.append({
            "customer_id": cid,
            "customer_name": fake.company(),
            "region": random.choice(regions),
            "industry": random.choice(industries),
            "email": fake.company_email(),
            "phone": fake.phone_number()
        })

    df = pd.DataFrame(customers)
    path = os.path.join(TODB, f"customers_{DATE_SUFFIX}.csv")
    df.to_csv(path, index=False, na_rep="", quoting=1)
    print(f"✅ Customers generated → {path}")
    return df

# ---------- MAIN ----------
def run():
    ar_df = generate_ar_invoices()
    upload_to_postgres(ar_df, "ar_invoices")

    payments_df = simulate_payments()

    customers_df = generate_customers()
    upload_to_postgres(customers_df, "customers")

if __name__ == "__main__":
    run()