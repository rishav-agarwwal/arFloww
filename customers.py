import pandas as pd
import psycopg2
import json
import os
import random
from faker import Faker
from datetime import datetime

# Constants
TODAY = datetime.today().date()
DATE_SUFFIX = TODAY.strftime("%Y%m%d")
OUTPUT_DIR = "D:\\Downloads\\AR Flow\\ToDB"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize Faker
fake = Faker()

# DB Config Loader
def load_db_config(path="D:\\Downloads\\AR Flow\\config\\dbconfig.json"):
    with open(path, "r") as f:
        return json.load(f)

# Fetch unique customer IDs from DB
def fetch_unique_customers():
    config = load_db_config()
    conn = psycopg2.connect(**config)
    query = "SELECT DISTINCT customer_id FROM ar_invoices"
    df = pd.read_sql(query, conn)
    conn.close()
    return df["customer_id"].tolist()

# Generate synthetic customer records
def generate_customer_data(customer_ids):
    industries = ["Retail", "Technology", "Manufacturing", "Finance", "Healthcare"]
    regions = ["NA", "EU", "APAC", "LATAM"]

    records = []
    for cid in customer_ids:
        records.append({
            "customer_id": cid,
            "customer_name": fake.company(),
            "region": random.choice(regions),
            "industry": random.choice(industries),
            "email": fake.company_email(),
            "phone": fake.phone_number()
        })
    return records

# Export to CSV
def export_to_csv(customers):
    df = pd.DataFrame(customers)
    path = os.path.join(OUTPUT_DIR, f"customers_{DATE_SUFFIX}.csv")
    df.to_csv(path, index=False, na_rep="", quoting=1)
    print(f"✅ Generated {len(customers)} customers → {path}")

if __name__ == "__main__":
    ids = fetch_unique_customers()
    customers = generate_customer_data(ids)
    export_to_csv(customers)