#!/usr/bin/env python3
"""
Clean CSV data by removing any bracketed segments (and the brackets) from all cells
EXCEPT column headers. Supports (), [], and {}. Operates on all CSV files within
an input folder (default: ./output_financial). By default, edits files in place.

Examples:
  Input cell: "Amount (in Rs.) [revised]"  ->  "Amount"
  Input cell: "John Doe {temp}"            ->  "John Doe"
  Input cell: "30-07-2013 (DoJ)"           ->  "30-07-2013"

Notes:
- Column headers are not modified.
- Empty results are left as empty strings; NaN stays NaN.
- Encoding defaults to UTF-8 with BOM ('utf-8-sig') and falls back automatically.
"""

import argparse
import os
import sys
import re
from typing import List, Tuple

import pandas as pd

BRACKET_PATTERNS = [
    re.compile(r"\([^()\[\]{}]*\)"),   # ( ... )
    re.compile(r"\[[^()\[\]{}]*\]"),   # [ ... ]
    re.compile(r"\{[^()\[\]{}]*\}"),   # { ... }
]

TRIM_EXCESS_SPACES = re.compile(r"\s{2,}")


def remove_bracketed(text: str) -> str:
    """Remove all bracketed segments and surrounding extra spaces from a string.
    Applies repeatedly to handle multiple segments. Does not handle deeply nested
    brackets, but is sufficient for common cases found in reports.
    """
    if not isinstance(text, str):
        return text

    s = text
    changed = True
    # Iteratively remove bracketed segments until no further change
    while changed:
        changed = False
        for pat in BRACKET_PATTERNS:
            new_s = pat.sub("", s)
            if new_s != s:
                s = new_s
                changed = True
    # Collapse multiple spaces and strip
    s = TRIM_EXCESS_SPACES.sub(" ", s).strip()
    return s


def clean_dataframe_cells(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """Return a copy of df with bracketed segments removed from data cells only.
    Column headers are preserved exactly. Returns (new_df, edits_count).
    """
    # Keep headers intact
    columns = list(df.columns)

    # Work on a copy to avoid mutating original
    new_df = df.copy()

    edits = 0
    for col in new_df.columns:
        # Only operate on object/string-like columns to avoid touching numerics
        series = new_df[col]
        if series.dtype == object or pd.api.types.is_string_dtype(series):
            def _clean_cell(v):
                nonlocal edits
                if pd.isna(v):
                    return v
                if isinstance(v, str):
                    cleaned = remove_bracketed(v)
                    if cleaned != v:
                        edits += 1
                    return cleaned
                return v
            new_df[col] = series.map(_clean_cell)
        else:
            # Leave numeric/boolean/datetime columns unchanged
            continue

    # Restore original headers exactly (defensive; pandas preserves already)
    new_df.columns = columns
    return new_df, edits


def find_csv_files(root: str, recursive: bool = True) -> List[str]:
    files: List[str] = []
    if recursive:
        for dirpath, _, filenames in os.walk(root):
            for fn in filenames:
                if fn.lower().endswith(".csv"):
                    files.append(os.path.join(dirpath, fn))
    else:
        for fn in os.listdir(root):
            p = os.path.join(root, fn)
            if os.path.isfile(p) and fn.lower().endswith(".csv"):
                files.append(p)
    return sorted(files)


def read_csv_with_fallbacks(path: str, encodings=("utf-8-sig", "utf-8", "latin-1")) -> Tuple[pd.DataFrame, str]:
    last_err = None
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            return df, enc
        except Exception as e:
            last_err = e
    raise last_err


def write_csv(df: pd.DataFrame, path: str, encoding: str):
    # Write without index and preserve encoding
    df.to_csv(path, index=False, encoding=encoding)


def main():
    parser = argparse.ArgumentParser(description="Remove bracketed content from CSV data cells (not headers)")
    parser.add_argument("--folder", default="output_financial", help="Folder containing CSV files (default: output_financial)")
    parser.add_argument("--no-recursive", action="store_true", help="Do not traverse subfolders")
    parser.add_argument("--dry-run", action="store_true", help="Analyze and report changes without writing files")
    parser.add_argument("--backup", action="store_true", help="Create a .bak copy before overwriting a CSV")
    args = parser.parse_args()

    folder = args.folder
    recursive = not args.no_recursive

    if not os.path.isdir(folder):
        print(f"Folder not found: {folder}")
        sys.exit(1)

    csv_files = find_csv_files(folder, recursive=recursive)
    if not csv_files:
        print(f"No CSV files found in {folder}{' (recursive)' if recursive else ''}.")
        sys.exit(0)

    total_files = 0
    total_edits = 0

    for csv_path in csv_files:
        try:
            df, used_enc = read_csv_with_fallbacks(csv_path)
        except Exception as e:
            print(f"[SKIP] Could not read: {csv_path} -> {e}")
            continue

        cleaned_df, edits = clean_dataframe_cells(df)
        if edits == 0:
            print(f"[OK] No changes: {csv_path}")
            total_files += 1
            continue

        total_files += 1
        total_edits += edits

        if args.dry_run:
            print(f"[DRY] Would edit {edits} cell(s): {csv_path}")
            continue

        # Backup if requested
        if args.backup:
            bak_path = csv_path + ".bak"
            try:
                if not os.path.exists(bak_path):
                    with open(csv_path, "rb") as r, open(bak_path, "wb") as w:
                        w.write(r.read())
                print(f"[BAK] {bak_path}")
            except Exception as e:
                print(f"[WARN] Failed to create backup for {csv_path}: {e}")

        # Write cleaned CSV
        try:
            write_csv(cleaned_df, csv_path, used_enc)
            print(f"[FIX] Edited {edits} cell(s): {csv_path}")
        except Exception as e:
            print(f"[ERR] Failed to write {csv_path}: {e}")

    print(f"\nDone. Processed {total_files} file(s), edited {total_edits} cell(s).")


if __name__ == "__main__":
    main()
