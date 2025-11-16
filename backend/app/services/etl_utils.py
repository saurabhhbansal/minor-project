# backend/app/services/etl_utils.py
import hashlib
import os
import re
import pandas as pd
from sqlalchemy import text

# -------------------- Utility: Compute SHA256 checksum --------------------
def sha256_of_file(path: str) -> str:
    """Compute SHA256 checksum for a given file path."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# -------------------- Transformation --------------------
def convert_wide_to_long(df_wide: pd.DataFrame, college_type: str) -> pd.DataFrame:
    """
    Converts the wide 25-column DF into long format with:
    College Name | College Type | Category | Year | Amount
    """
    if df_wide.empty:
        return pd.DataFrame(columns=["institution_name", "college_type", "category", "year", "amount"])

    college_name = df_wide.iloc[0]["College Name"]
    college_type = college_type.strip().upper()

    long_rows = []
    def clean_amount(val):
        """Extract numeric digits only; return int or None."""
        if val is None:
            return None
        s = str(val)
        nums = re.findall(r"[\d,]+", s)
        if not nums:
            return None
        num = nums[0].replace(",", "")
        try:
            return int(num)
        except:
            return None

    for col in df_wide.columns:
        if col == "College Name":
            continue

        value = df_wide[col].iloc[0]

        # Skip empty or missing values
        if value is None or str(value).strip() == "":
            continue

        # Extract Category & Year from "Category Name (2023-24)"
        match = re.match(r"(.+)\s*\((20\d{2}-\d{2})\)", col)
        if not match:
            continue

        category = match.group(1).strip()
        year = match.group(2).strip()
        clean_val = clean_amount(value)

        long_rows.append({
            "institution_name": college_name,
            "college_type": college_type,
            "category": category,
            "year": year,
            "amount": clean_val
        })

    long_df = pd.DataFrame(long_rows)
    return long_df



# -------------------- Load (Upsert to PostgreSQL) --------------------

def upsert_expenditures(df_long: pd.DataFrame, engine, source_pdf: str, pdf_sha256: str):
    """
    Upsert expenditure data into PostgreSQL.
    Adds college_type, pdf_sha256, and source_pdf fields.
    """

    df = df_long.copy()
    df['source_pdf'] = source_pdf
    df['pdf_sha256'] = pdf_sha256

    with engine.begin() as conn:
        # Ensure main table exists
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS expenditures (
            id BIGSERIAL PRIMARY KEY,
            institution_name TEXT,
            college_type TEXT,
            category TEXT,
            year VARCHAR(9),
            amount BIGINT,
            source_pdf TEXT,
            pdf_sha256 TEXT,
            created_at TIMESTAMP DEFAULT now(),
            UNIQUE (institution_name, category, year, pdf_sha256)
        );
        """))

        # ✅ Check if this file (pdf/csv) was already processed
        exists = conn.execute(
            text("SELECT 1 FROM expenditures WHERE pdf_sha256 = :sha LIMIT 1"),
            {"sha": pdf_sha256}
        ).fetchone()

        if exists:
            print(f"⚠️ Skipping {source_pdf} — already loaded (sha={pdf_sha256[:8]}...)")
            return  # ✅ Skip upload entirely

        # ✅ Drop temp table if it already exists
        conn.execute(text("DROP TABLE IF EXISTS tmp_expenditures;"))

        # ✅ Create a fresh temp table without id
        conn.execute(text("""
        CREATE TEMP TABLE tmp_expenditures AS
        SELECT institution_name, college_type, category, year, amount, source_pdf, pdf_sha256
        FROM expenditures
        WITH NO DATA;
        """))

        # ✅ Load data into temporary table
        df.to_sql("tmp_expenditures", con=conn, if_exists="append", index=False)

        # ✅ Perform upsert safely
        upsert_sql = """
        INSERT INTO expenditures (
            institution_name, college_type, category, year, amount, source_pdf, pdf_sha256, created_at
        )
        SELECT institution_name, college_type, category, year, amount, source_pdf, pdf_sha256, now()
        FROM tmp_expenditures
        ON CONFLICT (institution_name, category, year, pdf_sha256)
        DO UPDATE SET
            amount = EXCLUDED.amount,
            college_type = EXCLUDED.college_type,
            source_pdf = EXCLUDED.source_pdf;
        """
        conn.execute(text(upsert_sql))
        print(f"✅ Upserted {len(df)} rows from {source_pdf}")

