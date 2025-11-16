# Part 2: app/services/csv_manager.py
import os
import tempfile
import pandas as pd
from app.services import storage_utils
from supabase import StorageException

CSV_BUCKET = "csvs"

CSV_COLUMNS = [
    "College Name",
    "Library (2023-24)", "New Equipment for Laboratories (2023-24)",
    "Engineering Workshops (2023-24)", "Studios (2023-24)",
    "Other expenditure on creation of Capital Assets (2023-24)",
    "Library (2022-23)", "New Equipment for Laboratories (2022-23)",
    "Engineering Workshops (2022-23)", "Studios (2022-23)",
    "Other expenditure on creation of Capital Assets (2022-23)",
    "Library (2021-22)", "New Equipment for Laboratories (2021-22)",
    "Engineering Workshops (2021-22)", "Studios (2021-22)",
    "Other expenditure on creation of Capital Assets (2021-22)",
    "Salaries (2023-24)", "Maintenance of Academic Infrastructure or consumables and other running expenditures (2023-24)",
    "Seminars/Conferences/Workshops (2023-24)",
    "Salaries (2022-23)", "Maintenance of Academic Infrastructure or consumables and other running expenditures (2022-23)",
    "Seminars/Conferences/Workshops (2022-23)",
    "Salaries (2021-22)", "Maintenance of Academic Infrastructure or consumables and other running expenditures (2021-22)",
    "Seminars/Conferences/Workshops (2021-22)"
]
def _tmp_path_for(filename: str) -> str:
    base = tempfile.gettempdir()
    # ensure base exists (Windows? etc.)
    os.makedirs(base, exist_ok=True)
    # sanitize simple: replace slashes/backslashes
    safe = filename.replace("/", "_").replace("\\", "_")
    return os.path.join(base, safe)

def upload_pdf_safe(bucket, storage_path, local_path):
    try:
        print("\n=== Uploading PDF to Supabase ===")
        storage_utils.upload_file(
            bucket=bucket,
            local_path=local_path,
            storage_path=storage_path,
            upsert="false"
        )
        print("✅ PDF uploaded successfully.")
        return {"uploaded": True, "path": storage_path}

    except StorageException as e:   # <-- FIXED
        # Duplicate file → NON-FATAL
        if "resource already exists" in str(e).lower():
            print("⚠ PDF already exists in storage → skipping upload")
            return {"uploaded": False, "reason": "duplicate"}

        # Other errors → Fatal
        print("❌ Storage API error:", e)
        raise

    except Exception as e:
        print("❌ Unexpected storage error:", e)
        raise

def ensure_csv_for_college_type(college_type: str):
    csv_filename = f"NIRF_{college_type.replace(' ', '_')}'S_output.csv"
    local_csv = _tmp_path_for(csv_filename)

    # If file exists remotely, download
    if storage_utils.file_exists(CSV_BUCKET, csv_filename):
        print(f"📥 CSV exists for {college_type}, downloading...")
        storage_utils.download_file(CSV_BUCKET, csv_filename, local_csv)
        return local_csv, csv_filename

    print(f"📄 CSV NOT found for {college_type}, creating new local CSV: {local_csv}")

    df = pd.DataFrame(columns=CSV_COLUMNS)
    df.to_csv(local_csv, index=False)

    # ❌ Remove upload here
    # storage_utils.upload_file(...)

    return local_csv, csv_filename


def append_row_to_csv(local_csv_path: str, row: list, csv_name: str):
    """
    Append a row to the local CSV and upload it to Supabase.
    If upload fails (e.g., duplicate resource conflict), the underlying exception is allowed to bubble up.
    """
    # basic validations
    if not os.path.exists(local_csv_path):
        raise FileNotFoundError(f"Local CSV path not found: {local_csv_path}")

    # read existing into df
    df = pd.read_csv(local_csv_path)
    # append row (ensure length matches)
    if len(row) != len(df.columns):
        # If columns count mismatch, try to align (pad or trim)
        if len(row) < len(df.columns):
            row = row + [""] * (len(df.columns) - len(row))
        else:
            row = row[:len(df.columns)]
    df.loc[len(df)] = row

    # write back to local
    df.to_csv(local_csv_path, index=False)

    # debug print (you can remove later)
    print(f"⬆ Uploading updated CSV → Supabase: {csv_name}")
    # upload (expect storage_utils.upload_file to either succeed or raise StorageApiError)
    storage_utils.upload_file(CSV_BUCKET, local_csv_path, csv_name)
    print("✅ Upload complete.")
