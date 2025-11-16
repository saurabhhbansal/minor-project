# backend/app/db_init.py
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL, connect_args={"sslmode": "require"})

def create_metadata_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS uploaded_files (
        id BIGSERIAL PRIMARY KEY,
        filename TEXT NOT NULL,
        bucket TEXT NOT NULL,
        path TEXT NOT NULL,
        sha256 TEXT NOT NULL UNIQUE,
        college_type TEXT,
        uploaded_by TEXT,
        storage_url TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

    print("✅ uploaded_files metadata table ready.")

if __name__ == "__main__":
    create_metadata_table()
