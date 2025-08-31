import psycopg2
from backend.db_connector import connect_db

def create_snapshot_table():
    """Creates the ar_daily_snapshot table for historical trend analysis."""
    conn = connect_db()
    cur = conn.cursor()
    
    query = """
    CREATE TABLE IF NOT EXISTS ar_daily_snapshot (
        snapshot_date DATE PRIMARY KEY,
        total_outstanding NUMERIC(15, 2),
        overdue_amount NUMERIC(15, 2),
        dso NUMERIC(10, 2),
        invoice_count INT
    );
    """
    
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ 'ar_daily_snapshot' table created or already exists.")

if __name__ == "__main__":
    create_snapshot_table()