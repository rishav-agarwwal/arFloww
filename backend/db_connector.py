import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

def connect_db():
    """
    Connects to the PostgreSQL database using environment variables.
    Returns a psycopg2 connection object or None on failure.
    """
    try:
        connection = psycopg2.connect(
            user=os.getenv("user"),
            password=os.getenv("password"),
            host=os.getenv("host"),
            port=os.getenv("port"),
            dbname=os.getenv("dbname")
        )
        return connection
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return None