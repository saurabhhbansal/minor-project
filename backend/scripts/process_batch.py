# Part 3: scripts/process_batch.py
import os
import re
import hashlib
import logging
import pandas as pd
from typing import Optional
from sqlalchemy import create_engine, text
from app.services import csv_manager, storage_utils,extraction_final_long  # assumes these modules exist
from app.services import etl_utils  # your existing etl utils
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DATABASE_URL = os.environ.get("DATABASE_URL")

def sha256_of_file(path: str) -> Optional[str]:
    if not path or not os.path.exists(path): return None
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def append_to_csv_wide_df(
    df_wide: pd.DataFrame,
    pdf_filename: str,
    college_type: str,
    pdf_local_path: Optional[str] = None
):
    """
    FINAL version:
    Accepts a WIDE-FORMAT DataFrame (already mapped).
    Does NOT do mapping. Does NOT call extract_pdf_to_records().

    Steps:
      1. compute SHA of PDF
      2. check uploaded_files for duplicates
      3. ensure college CSV exists in Supabase
      4. append wide row to CSV
      5. record metadata in uploaded_files
    """

    if DATABASE_URL is None:
        raise RuntimeError("DATABASE_URL not set in environment.")

    engine = create_engine(DATABASE_URL)

    # ------------------------------------------------------------
    # 1) SHA computation
    # ------------------------------------------------------------
    sha256 = None
    try:
        path_for_sha = None
        if pdf_local_path and os.path.exists(pdf_local_path):
            path_for_sha = pdf_local_path
        elif os.path.exists(pdf_filename):
            path_for_sha = pdf_filename

        if path_for_sha:
            sha256 = sha256_of_file(path_for_sha)

    except Exception:
        sha256 = None

    # ------------------------------------------------------------
    # 2) Duplicate check — using SHA
    # ------------------------------------------------------------
    if sha256:
        try:
            with engine.begin() as conn:
                exists = conn.execute(
                    text("SELECT 1 FROM uploaded_files WHERE sha256 = :sha"),
                    {"sha": sha256}
                ).fetchone()

            if exists:
                raise RuntimeError(
                    f"Duplicate PDF detected (sha={sha256[:10]}). Already processed."
                )

        except RuntimeError:
            raise
        except Exception as e:
            LOG.warning("Could not check uploaded_files table: %s", e)

    # ------------------------------------------------------------
    # 3) Ensure CSV exists
    # ------------------------------------------------------------
    local_csv_path, csv_name = csv_manager.ensure_csv_for_college_type(college_type)

    # ------------------------------------------------------------
    # 4) Append the wide-format row
    # ------------------------------------------------------------

    # df_wide is a 1-row dataframe → convert to list
    row_data = df_wide.iloc[0].tolist()

    # convert booleans → strings
    row_data = [str(x) if isinstance(x, bool) else x for x in row_data]

    try:
        csv_manager.append_row_to_csv(local_csv_path, row_data, csv_name)
    except Exception:
        raise   # let API return the error (duplicate, storage error, etc.)

    # ------------------------------------------------------------
    # 5) Insert metadata
    # ------------------------------------------------------------
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO uploaded_files (filename, bucket, path, sha256, college_type)
                    VALUES (:f, :b, :p, :s, :c)
                    ON CONFLICT (sha256) DO NOTHING
                """),
                {
                    "f": pdf_filename,
                    "b": "csvs",
                    "p": csv_name,
                    "s": sha256 or "",
                    "c": college_type,
                }
            )
    except Exception as e:
        LOG.warning("Failed to save metadata row: %s", e)

    return {"csv": csv_name, "row_count": 1}


# --------------------- Single-file processing ---------------------
def process_single_pdf(pdf_path: str, engine, college_type: Optional[str] = None, original_filename: Optional[str] = None):
    pdf_name = original_filename  # CORRECT
    LOG.info("=== SINGLE PDF MODE === Processing %s", pdf_name)

    if not college_type:
        raise ValueError("college_type is required for single PDF processing.")

    # ---------------------------------------------------------
    # 1) Extract wide DF
    # ---------------------------------------------------------
    df_wide = extraction_final_long.extract_financial_data(pdf_path, college_type, original_filename)

    # Also create long DF for DB insert
    df_long = etl_utils.convert_wide_to_long(df_wide, college_type)

    if df_long is None or df_long.empty:
        LOG.warning("Extractor returned empty data for %s. Skipping.", pdf_name)
        return {"status": "no_data", "pdf": pdf_name}

    # ---------------------------------------------------------
    # 2) Duplicate check **ONLY for DB**, based on institution_name
    # ---------------------------------------------------------
    college_name = df_long.iloc[0]["institution_name"]

    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM expenditures WHERE institution_name = :n"),
            {"n": college_name},
        ).fetchone()

    duplicate_db = exists is not None

    if duplicate_db:
        LOG.warning("⚠ Duplicate in DB: %s (skipping DB insert)", college_name)
    else:
        # ---------------------------------------------------------
        # 3) Insert into DB if not duplicate
        # ---------------------------------------------------------
        pdf_sha = sha256_of_file(pdf_path)

        try:
            etl_utils.upsert_expenditures(df_long, engine, pdf_name, pdf_sha)
            LOG.info("DB upsert successful for %s", pdf_name)
        except Exception as e:
            LOG.exception("DB insert failed: %s", e)
            # continue anyway – CSV should still be uploaded
    # ----- UPLOAD PDF to Supabase -----
    folder_name = f"NIRF {college_type.upper()}'S"
    pdf_storage_path = f"{folder_name}/{pdf_name}"
    try:
        upload_result = csv_manager.upload_pdf_safe(
            bucket="pdfs",
            storage_path=pdf_storage_path,
            local_path=pdf_path
        )
        LOG.info("PDF upload result: %s", upload_result)
    except Exception as e:
        LOG.exception("PDF upload failed: %s", e)
        raise RuntimeError(f"PDF upload failed: {e}")
    # ---------------------------------------------------------
    # 4) CSV upload MUST NOT depend on DB insert
    # ---------------------------------------------------------
    try:
        csv_result = append_to_csv_wide_df(df_wide, pdf_name, college_type, pdf_local_path=pdf_path)
    except Exception as e:
        LOG.exception("CSV upload failed: %s", e)
        raise RuntimeError(f"CSV upload failed: {e}")

    # ---------------------------------------------------------
    # 5) Final output
    # ---------------------------------------------------------
    return {
        "status": "processed_with_duplicate_db" if duplicate_db else "processed",
        "db_duplicate": duplicate_db,
        "pdf": pdf_name,
        "pdf_upload_result": upload_result,
        "csv_result": csv_result,
    }


# --------------------- Folder mode ---------------------
def process_folder(folder_path: str, engine, college_type: Optional[str] = None, original_filename: Optional[str] = None):
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    if not college_type:
        raise ValueError("college_type is required for folder processing.")

    pdf_files = [
        f for f in os.listdir(folder_path)
        if f.lower().endswith(".pdf")
    ]
    pdf_files.sort()

    results = []

    for pdf_file in pdf_files:
        pdf_path = os.path.join(folder_path, pdf_file)

        try:
            res = process_single_pdf(
                pdf_path,
                engine,
                college_type=college_type,
                original_filename=original_filename
            )
            results.append({"file": pdf_file, "result": res})

        except Exception as e:
            LOG.exception("❌ Error processing %s: %s", pdf_file, e)
            results.append({"file": pdf_file, "error": str(e)})

    return results

# --------------------- process_batch router ---------------------
def connect_db(db_url: str):
    LOG.info("Connecting to database...")
    eng = create_engine(db_url, connect_args={"sslmode": "require"})
    LOG.info("Database connection OK.")
    return eng

def process_batch(input_path: Optional[str] = None, college_type: Optional[str] = None,original_filename: str = None):
    LOG.info("\n=== 🚀 STARTING BATCH PROCESS ===")

    if not input_path:
        raise ValueError("input_path is required")

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Path does not exist: {input_path}")

    # DB connection
    engine = connect_db(DATABASE_URL)

    # SINGLE PDF
    if os.path.isfile(input_path) and input_path.lower().endswith(".pdf"):
        LOG.info("📄 Detected: SINGLE PDF upload")
        return process_single_pdf(
            input_path,
            engine,
            college_type=college_type,
            original_filename=original_filename
        )

    # FOLDER
    if os.path.isdir(input_path):
        LOG.info("📁 Detected: FOLDER upload")
        return process_folder(
            input_path,
            engine,
            college_type=college_type,
            original_filename=original_filename
        )

    raise ValueError("Unsupported input provided to process_batch")

