"""
PDF Table Extractor using Camelot
This script extracts tables from PDF files and displays them as pandas DataFrames.
"""

import camelot
import pandas as pd
import os
import sys
from pathlib import Path
import fitz  # PyMuPDF
import re


def _sanitize_heading(text: str) -> str:
    text = text.strip()
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text)
    # Trim overly long headings
    return text[:160]


def _to_fitz_y(page, camelot_y: float) -> float:
    """Convert Camelot (PDF) y to fitz y using page height."""
    return float(page.rect.height) - float(camelot_y)


def _find_heading_above(page, bbox, band_px: int = 120) -> str | None:
    """Find the closest text block within a vertical band just above the table top.
    - bbox: Camelot bbox (x_left, y_bottom, x_right, y_top) in PDF coords
    - band_px: search height in pixels/points above table top
    Returns heading text or None.
    """
    try:
        x_left, y_bottom, x_right, y_top = bbox
    except Exception:
        return None
    table_top_fitz = _to_fitz_y(page, y_top)
    blocks = page.get_text("blocks") or []
    candidates = []
    for b in blocks:
        x0, y0, x1, y1, text, *_ = b
        if not text or not text.strip():
            continue
        # Consider blocks with bottom y1 just above table_top within band
        if y1 <= table_top_fitz and (table_top_fitz - y1) <= band_px:
            s = text.strip()
            # Heuristics: short-ish, not mostly digits/symbols
            if 2 <= len(s) <= 140:
                # digit ratio
                digits = sum(c.isdigit() for c in s)
                letters = sum(c.isalpha() for c in s)
                if letters == 0 and digits > 0:
                    continue
                if digits / max(1, len(s)) > 0.4:
                    continue
                # Prefer blocks horizontally overlapping table width somewhat
                horiz_overlap = max(0, min(x1, x_right) - min(x1, x_right) - max(0, (min(x1, x_right) - max(x0, x_left))))
                # Simple score by proximity (smaller distance better)
                distance = table_top_fitz - y1
                candidates.append((distance, s))
    if not candidates:
        return None
    # Choose by smallest vertical distance
    candidates.sort(key=lambda t: t[0])
    return _sanitize_heading(candidates[0][1])


def extract_tables_from_pdf(pdf_path, pages='all'):
    """
    Extract tables from a PDF file using Camelot, detect headings just above,
    and merge tables without headings into the last heading group.

    Returns dict: heading -> merged DataFrame
    """
    try:
        # Check if PDF file exists
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")

        print(f"Extracting tables from: {pdf_path}")

        # Extract tables using Camelot (lattice only)
        tables = camelot.read_pdf(
            pdf_path,
            pages=pages,
            flavor='lattice',
            line_scale=40,
            copy_text=['h', 'v'],
            shift_text=['l', 't'],
            strip_text='\n'
        )

        if len(tables) == 0:
            print("No tables found in the PDF.")
            return {}

        print(f"Found {len(tables)} table(s)")

        # Open the PDF with fitz
        doc = fitz.open(pdf_path)

        # Build sortable metadata for each table
        meta = []
        for t in tables:
            try:
                page_idx = int(t.page) - 1
            except Exception:
                page_idx = 0
            page = doc[page_idx]
            x_left, y_bottom, x_right, y_top = t._bbox
            top_fitz = _to_fitz_y(page, y_top)
            meta.append({
                "table": t,
                "page_idx": page_idx,
                "page": page,
                "bbox": (x_left, y_bottom, x_right, y_top),
                "top_fitz": top_fitz,
            })
        # Process sequentially: page ascending, then visual top-to-bottom
        meta.sort(key=lambda m: (m["page_idx"], m["top_fitz"]))
        merged: dict[str, pd.DataFrame] = {}
        last_heading: str | None = None
        for i, m in enumerate(meta, start=1):
            tbl = m["table"]
            df = tbl.df
            if df is None or len(df) == 0:
                continue
            heading = _find_heading_above(m["page"], m["bbox"], band_px=140)
            if heading:
                last_heading = heading
                if heading in merged:
                    merged[heading] = pd.concat([merged[heading], df], ignore_index=True)
                else:
                    merged[heading] = df
                print(f"Assigned Table {i} to heading: {heading}")
            else:
                target = last_heading or "Unlabeled"
                if target in merged:
                    merged[target] = pd.concat([merged[target], df], ignore_index=True)
                else:
                    merged[target] = df
                print(f"No heading found for Table {i}; merged into: {target}")
        # Optional: print summaries
        for h, d in merged.items():
            print(f"\nHeading: {h} -> Rows: {len(d)}, Cols: {len(d.columns)}")
        return merged
    except Exception as e:
        print(f"Error extracting tables: {str(e)}")
        return {}


def save_tables_to_csv(merged_tables, output_dir="output", pdf_name="extracted_tables"):
    """
    Save merged tables to CSV files. Filenames are heading-based.
    """
    if not merged_tables:
        print("No tables to save.")
        return
    Path(output_dir).mkdir(exist_ok=True)
    for heading, df in merged_tables.items():
        safe = heading.replace('/', '_').replace('\\', '_').replace(':', '_').replace('|', '_')
        safe = safe.replace('\n', ' ').replace('\r', ' ')
        safe = re.sub(r"[^\w\s-]", "_", safe)
        safe = re.sub(r"\s+", "_", safe).strip('_')
        filename = f"{output_dir}/{safe}.csv"
        df.to_csv(filename, index=False)
        print(f"Saved: {filename}")


def main():
    """
    Main function to demonstrate PDF table extraction.
    """
    print("PDF Table Extractor using Camelot")
    print("=" * 40)

    # Example usage - you can modify this path
    pdf_path = "iit_delhi_data.pdf"

    if not pdf_path:
        print("No PDF path provided.")
        return

    # Extract tables and save them to CSV
    print("\nExtracting tables using lattice method...")
    merged = extract_tables_from_pdf(pdf_path)
    if not merged:
        print("No tables could be extracted with lattice method.")
        return
    save_tables_to_csv(merged, pdf_name=Path(pdf_path).stem)


if __name__ == "__main__":
    main()
