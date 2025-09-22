"""
PDF Table Extractor with corrected top-to-bottom sorting,
improved multi-page merging logic (no same-page merge),
and a debug mode for tracing merge decisions.
"""

import camelot
import pandas as pd
import os
import re
import fitz
from pathlib import Path
import time
from google.api_core import exceptions
import google.generativeai as genai

# --- Helper Functions ---

def _sanitize_text(text: str) -> str:
    """Cleans up text to be used as a filename or key."""
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    return text[:160]

def _to_fitz_y(page, camelot_y: float) -> float:
    """Converts Camelot's y-coordinate to PyMuPDF's y-coordinate."""
    return float(page.rect.height) - float(camelot_y)

def _horizontal_overlap(bbox1, bbox2):
    """Calculates the horizontal overlap ratio of two bounding boxes."""
    x1_l, _, x1_r, _ = bbox1
    x2_l, _, x2_r, _ = bbox2
    
    if x1_r < x2_l or x2_r < x1_l:
        return 0
    
    intersection_width = min(x1_r, x2_r) - max(x1_l, x2_l)
    width1 = x1_r - x1_l
    width2 = x2_r - x2_l
    
    return intersection_width / min(width1, width2) if min(width1, width2) > 0 else 0

def _find_heading_above(page, bbox, band_px: int = 120) -> str | None:
    """
    Finds a likely heading text block just above a table's bounding box.
    """
    try:
        _, _, _, y_top = bbox
    except (TypeError, ValueError):
        return None
    
    table_top_fitz = _to_fitz_y(page, y_top)
    blocks = page.get_text("blocks") or []
    candidates = []
    
    for b in blocks:
        x0, y0, x1, y1, text, *_ = b
        if not text or not text.strip():
            continue
            
        if y1 <= table_top_fitz and (table_top_fitz - y1) <= band_px:
            s = text.strip()
            if 2 <= len(s) <= 140:
                digits = sum(c.isdigit() for c in s)
                letters = sum(c.isalpha() for c in s)
                if letters == 0 and digits > 0:
                    continue
                if digits / max(1, len(s)) > 0.4:
                    continue
                distance = table_top_fitz - y1
                candidates.append((distance, s))
                
    if not candidates:
        return None
        
    candidates.sort(key=lambda t: t[0])
    return _sanitize_text(candidates[0][1])

# --- Core Extraction and Grouping Logic ---

def extract_and_group_tables(pdf_path, pages='all', min_rows=1, min_cols=1, debug=False):
    """
    Extracts tables with Camelot and then groups them across pages
    using a relaxed, heading-aware merging logic.
    """
    try:
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
            
        print(f"Extracting tables from: {pdf_path} using Camelot...")
        
        tables = camelot.read_pdf(
            pdf_path,
            pages=pages,
            flavor='lattice',
            line_scale=40
        )
        
        if not tables:
            print("No tables found by Camelot.")
            return {}
            
        print(f"Found {len(tables)} tables. Filtering and grouping...")
        filtered_tables = [t for t in tables if len(t.df) >= min_rows and len(t.df.columns) >= min_cols]
        filtered_tables.sort(key=lambda t: (int(t.page), -t._bbox[3], t._bbox[0]))

        groups = []
        doc = fitz.open(pdf_path)

        for idx, t in enumerate(filtered_tables, start=1):
            current_page = int(t.page)
            page = doc[current_page - 1]

            heading = _find_heading_above(page, t._bbox, band_px=140)

            if debug:
                print(f"\n[Table {idx}] Page {current_page}, BBox={t._bbox}")
                print(f"  Detected heading: {heading if heading else 'None'}")

            if groups:
                last_group = groups[-1]
                last_table_in_group = last_group[-1]
                last_page = int(last_table_in_group.page)

                if heading:
                    groups.append([t])
                    if debug:
                        print("  → New group started (heading found).")
                else:
                    overlap = _horizontal_overlap(t._bbox, last_table_in_group._bbox)
                    if current_page == last_page + 1 and overlap > 0.5:
                        last_group.append(t)
                        if debug:
                            print(f"  → Merged with previous group (page {last_page} → {current_page}, overlap={overlap:.2f}).")
                    else:
                        groups.append([t])
                        if debug:
                            print(f"  → New group started (no heading, overlap={overlap:.2f}, pages not consecutive).")
            else:
                groups.append([t])
                if debug:
                    print("  → First table, new group started.")

        final_tables = {}
        for i, group in enumerate(groups):
            merged_df = pd.DataFrame()
            
            group.sort(key=lambda t: (int(t.page), -t._bbox[3], t._bbox[0]))
            
            for t in group:
                merged_df = pd.concat([merged_df, t.df], ignore_index=True)
            
            key = f"Table_Group_{i+1}_Page_{group[0].page}"
            final_tables[key] = merged_df

        # Print summary
        for h, d in final_tables.items():
            print(f"\nGroup: {h} -> Rows: {len(d)}, Cols: {len(d.columns)}")
        
        return final_tables
        
    except Exception as e:
        print(f"Error extracting tables: {str(e)}")
        return {}

# --- Main Functions ---

def save_tables_to_csv(merged_tables, output_dir="output"):
    """Saves the grouped tables to CSV files."""
    if not merged_tables:
        print("No tables to save.")
        return
    Path(output_dir).mkdir(exist_ok=True)
    for heading, df in merged_tables.items():
        safe_heading = re.sub(r"[^\w\s-]", "", heading)
        safe_heading = re.sub(r"\s+", "_", safe_heading).strip('_')
        filename = f"{output_dir}/{safe_heading}.csv"
        df.to_csv(filename, index=False)
        print(f"Saved: {filename}")

def call_gemini_api(csv_content: str, api_key: str) -> str | None:
    """Calls the Gemini API to clean the CSV content."""
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.5-flash-lite') # Using the latest available flash model
        prompt = f"Unpivot this CSV into a two-column format. The first column should be 'Description', and the second should be 'Value'. Combine all relevant headers and row names to create a descriptive label in the 'Description' column. Exclude any empty or null values.\n\n{csv_content}"
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Error calling Gemini API: {e}")
        return None

def process_csvs_with_gemini(api_key: str, output_dir="output"):
    """Processes all CSV files in the output directory with the Gemini API."""
    print("\nStarting Gemini CSV processing...")
    if not api_key:
        print("API key is missing. Skipping Gemini processing.")
        return

    csv_files = [f for f in os.listdir(output_dir) if f.endswith(".csv") and "cleaned" not in f]
    for i, filename in enumerate(csv_files):
        csv_path = os.path.join(output_dir, filename)
        print(f"Processing {csv_path}...")
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            csv_data = f.read()
        
        cleaned_csv = call_gemini_api(csv_data, api_key)
        
        if cleaned_csv:
            base, ext = os.path.splitext(filename)
            new_filename = f"{base}_cleaned{ext}"
            new_filepath = os.path.join(output_dir, new_filename)
            
            # Clean up the response from Gemini
            # It sometimes adds backticks for code blocks
            cleaned_csv = cleaned_csv.strip().strip("`").strip()
            if cleaned_csv.lower().startswith('csv'):
                cleaned_csv = cleaned_csv[3:].lstrip()


            with open(new_filepath, 'w', encoding='utf-8') as f:
                f.write(cleaned_csv)
            print(f"Saved cleaned file: {new_filepath}")

        if i < len(csv_files) - 1:
            print("Waiting 2 seconds before next request...")
            time.sleep(2) # Wait for 2 seconds to respect 30 RPM limit

def main(debug=False):
    pdf_path = "NIRF_IITBombay_2025_Overall_Category.pdf"
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found at '{pdf_path}'. Please make sure the file is in the same directory.")
        return

    api_key = input("Please enter your Gemini API key: ").strip()
    
    print("\nStarting table extraction and grouping...")
    merged = extract_and_group_tables(pdf_path, debug=debug)
    
    if not merged:
        print("No tables could be extracted or merged.")
    else:
        save_tables_to_csv(merged)
        process_csvs_with_gemini(api_key)

if __name__ == "__main__":
    main(debug=True)  # Enable debug mode
