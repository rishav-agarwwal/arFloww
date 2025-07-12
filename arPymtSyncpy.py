import psycopg2
import json

# Load DB config
def load_db_config(path="D:\\Downloads\\AR Flow\\config\\dbconfig.json"):
    with open(path, "r") as f:
        return json.load(f)

# Execute status update
def update_invoice_status():
    config = load_db_config()
    conn = psycopg2.connect(**config)
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

    print(f"✅ Updated {affected} records in ar_invoices table")

if __name__ == "__main__":
    update_invoice_status()