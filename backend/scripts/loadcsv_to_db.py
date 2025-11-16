# backend/scripts/load_csv_to_db.py

import os
import sys
import time
import hashlib
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# allow imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.services import etl_utils
from dotenv import load_dotenv
load_dotenv()

# --------------- CONFIG ----------------
DATA_DIR = r"C:\Users\Nitima\minor-3\data"
DATABASE_URL = os.environ.get("DATABASE_URL")

# CSV files to load
CSV_FILES = [
    ("NIRF_IITs_output.csv", "IIT"),
    ("NIRF_NITs_output.csv", "NIT"),
    ("NIRF_IIITs_output.csv", "IIIT"),
    ("NIRF_IISERs_output.csv", "IISER")
]

# -----------------------------
# Retry-safe Neon connector
# -----------------------------
def connect_with_retry(db_url, retries=6, delay=5):
    """Create SQLAlchemy engine with retry to handle Neon cold starts."""
    for attempt in range(1, retries + 1):
        try:
            engine = create_engine(
                db_url,
                pool_pre_ping=True
            )
            conn = engine.connect()
            conn.close()
            print(f"✅ Database connection established on attempt {attempt}")
            return engine
        except OperationalError:
            print(f"⚠️ Neon is waking up (attempt {attempt}). Retrying in {delay} sec...")
            time.sleep(delay)

    raise Exception("❌ Could not connect to Neon after several retries! Check DB URL or network.")


# -----------------------------
# Compute SHA256 for CSV file
# -----------------------------
def sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# -----------------------------
# MAIN SCRIPT
# -----------------------------
def main():

    # connect safely
    engine = connect_with_retry(DATABASE_URL)
    print(f"🔌 Connected to database: {DATABASE_URL}")

    for file_name, college_type in CSV_FILES:
        file_path = os.path.join(DATA_DIR, file_name)

        if not os.path.exists(file_path):
            print(f"⚠️ Skipping {file_name} (file not found)")
            continue

        print(f"\n📂 Processing {file_name} for {college_type}")

        # SHA of CSV to check if already processed
        csv_sha = sha256_of_file(file_path)

        # Check if already inserted
        with engine.begin() as conn:
            result = conn.execute(
                text("SELECT COUNT(*) FROM expenditures WHERE pdf_sha256 = :sha"),
                {"sha": csv_sha}
            ).scalar()

        if result > 0:
            print(f"⏩ Skipping {file_name} — already uploaded earlier (sha match).")
            continue

        # Load CSV
        df = pd.read_csv(file_path)

        # Transform
        try:
            df_long = etl_utils.transform_to_long_format(df, college_type=college_type)
        except Exception as e:
            print(f"❌ Transformation failed for {file_name}: {e}")
            continue

        # Store in DB
        try:
            etl_utils.upsert_expenditures(
                df_long,
                engine,
                source_pdf=file_name,
                pdf_sha256=csv_sha
            )
            print(f"✅ Successfully upserted {len(df_long)} rows from {file_name}")

        except Exception as e:
            print(f"❌ Database insert failed for {file_name}: {e}")


if __name__ == "__main__":
    main()
