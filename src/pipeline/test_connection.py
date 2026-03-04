import psycopg2
import os
from dotenv import load_dotenv
from psycopg2 import OperationalError

# Load .env file
load_dotenv()

def test_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            dbname=os.getenv("DB_NAME")
        )
        print("✅ Connection successful!")
        conn.close()
    except OperationalError as e:
        print("❌ Connection failed:")
        print(e)

if __name__ == "__main__":
    test_connection()
