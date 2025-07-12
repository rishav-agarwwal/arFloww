import pandas as pd
import random
from datetime import datetime, timedelta
import os
from faker import Faker

# Initialize Faker
fake = Faker()

# Set constants
NUM_INVOICES = random.randint(100, 2500)
TODAY = datetime.today().date()
OUTPUT_DIR = "D:\\Downloads\\AR Flow\\ToDB"
DATE_SUFFIX = TODAY.strftime("%Y%m%d")
OUTPUT_FILE = f"ar_invoices_{DATE_SUFFIX}.csv"

# Ensure ToDB directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Generate invoice data
def generate_invoice_data(n):
    invoices = []
    for i in range(n):
        invoice_no = f"INV{str(i + 1).zfill(7)}"
        customer_id = f"CUS{str(random.randint(1, 99999)).zfill(5)}"
        amount = round(random.uniform(100, 999999), 2)
        create_date = random.choice(TODAY)  # Random create date within the last year
        due_date = create_date + timedelta(days=random.choice([30, 60]))  # Randomly set due date to 30 or 60 days later
        status = 1  # 1 = Open

        invoices.append({
            "invoice_no": invoice_no,
            "customer_id": customer_id,
            "amountall": amount,
            "create_date": create_date,
            "due_date": due_date,
            "status": status
        })
    return invoices

# Generate and save to CSV
def save_to_csv(data, file_path):
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    print(f"✅ Generated {len(data)} AR invoices → {file_path}")

if __name__ == "__main__":
    data = generate_invoice_data(NUM_INVOICES)
    save_to_csv(data, os.path.join(OUTPUT_DIR, OUTPUT_FILE))
