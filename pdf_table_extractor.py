"""
PDF Table Extractor with advanced header analysis to handle all table structures,
prevent duplicate headers, and correctly process all data.
"""

import camelot
import pandas as pd
import os
import re
import fitz  # PyMuPDF
from typing import Optional, Tuple
from openpyxl.utils import get_column_letter

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
    if x1_r < x2_l or x2_r < x1_l: return 0
    intersection_width = min(x1_r, x2_r) - max(x1_l, x2_l)
    width1 = x1_r - x1_l
    width2 = x2_r - x2_l
    return intersection_width / min(width1, width2) if min(width1, width2) > 0 else 0

def _find_heading_above(page, bbox, band_px: int = 120) -> Optional[str]:
    """Finds a likely heading text block just above a table's bounding box."""
    try: _, _, _, y_top = bbox
    except (TypeError, ValueError): return None
    table_top_fitz = _to_fitz_y(page, y_top)
    blocks = page.get_text("blocks") or []
    candidates = []
    for b in blocks:
        try: x0, y0, x1, y1, text, *_ = b
        except ValueError: continue
        if not text or not text.strip(): continue
        if y1 <= table_top_fitz and (table_top_fitz - y1) <= band_px:
            s = text.strip()
            if 2 <= len(s) <= 140:
                digits = sum(c.isdigit() for c in s)
                letters = sum(c.isalpha() for c in s)
                if letters == 0 and digits > 0: continue
                if digits / max(1, len(s)) > 0.4: continue
                distance = table_top_fitz - y1
                candidates.append((distance, s))
    if not candidates: return None
    candidates.sort(key=lambda t: t[0])
    return _sanitize_text(candidates[0][1])

# --- Core Extraction and Grouping Logic ---

def extract_and_group_tables(pdf_path, pages='all', min_rows=1, min_cols=1, debug=False):
    """Extracts tables with Camelot and then groups them across pages."""
    try:
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        print(f"Extracting tables from: {pdf_path} using Camelot...")
        tables = camelot.read_pdf(pdf_path, pages=pages, flavor='lattice', line_scale=40)
        if not tables:
            print("No tables found by Camelot."); return {}
        print(f"Found {len(tables)} tables. Filtering and grouping...")
        filtered_tables = [t for t in tables if len(t.df) >= min_rows and len(t.df.columns) >= min_cols]

        def safe_bbox(t):
            return getattr(t, "_bbox", t.parsing_report.get("bbox", (0, 0, 0, 0)))

        sorted_filtered_tables = sorted(filtered_tables, key=lambda t: (int(t.page), -safe_bbox(t)[3], safe_bbox(t)[0]))
        groups = []
        doc = fitz.open(pdf_path)

        for idx, t in enumerate(sorted_filtered_tables, start=1):
            current_page = int(t.page)
            page = doc[current_page - 1]
            bbox = safe_bbox(t)
            heading = _find_heading_above(page, bbox, band_px=140)
            t.heading = heading
            if debug: print(f"\n[Table {idx}] Page {current_page}, BBox={bbox}\n  Detected heading: {heading if heading else 'None'}")
            if groups:
                last_group = groups[-1]
                last_table_in_group = last_group[-1]
                last_page = int(last_table_in_group.page)
                if heading: groups.append([t])
                else:
                    overlap = _horizontal_overlap(bbox, safe_bbox(last_table_in_group))
                    if current_page == last_page + 1 and overlap > 0.5: last_group.append(t)
                    else: groups.append([t])
            else: groups.append([t])

        final_tables = {}
        for i, group in enumerate(groups):
            merged_df = pd.concat([t.df for t in group], ignore_index=True)
            group_heading = group[0].heading
            key = group_heading if group_heading else f"Table_Group_{i+1}_Page_{group[0].page}"
            original_key, counter = key, 1
            while key in final_tables:
                key = f"{original_key}_{counter}"; counter += 1
            final_tables[key] = merged_df
        doc.close()
        for h, d in final_tables.items(): print(f"\nGroup: '{h}' -> Rows: {len(d)}, Cols: {len(d.columns)}")
        return final_tables
    except Exception as e:
        print(f"Error extracting tables: {str(e)}"); return {}

# --- Function to Save Raw Tables for Debugging ---

def save_raw_tables_for_debug(tables_dict, output_filename="debug_raw_tables.xlsx"):
    """Saves each extracted DataFrame to a separate sheet in an Excel file for inspection."""
    print(f"\nSaving raw tables for debugging to '{output_filename}'...")
    try:
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            for table_name, df in tables_dict.items():
                safe_sheet_name = re.sub(r'[\\/*?:"<>|]', "", table_name)[:31]
                df.to_excel(writer, sheet_name=safe_sheet_name, index=False, header=False)
        print("✅ Debug file saved successfully.")
    except Exception as e:
        print(f"❌ Could not save debug file: {e}")

# --- <<< NEW: ADVANCED HEADER ANALYSIS FUNCTION >>> ---

def find_header_and_data_start(df: pd.DataFrame) -> Tuple[int, int, list]:
    """
    Analyzes the first few rows of a DataFrame to find the true header row,
    data start row, and constructs a merged, clean header.
    """
    def is_row_numerical_index(row):
        return all(str(item).strip() == str(i) for i, item in enumerate(row))
    
    start_offset = 0
    if is_row_numerical_index(df.iloc[0]):
        start_offset = 1

    best_header_index = -1
    max_text_cells = -1

    # Find the row with the most non-empty text cells in the first 4 rows
    for i in range(start_offset, min(start_offset + 4, len(df))):
        text_cells = sum(1 for cell in df.iloc[i] if str(cell).strip())
        if text_cells > max_text_cells:
            max_text_cells = text_cells
            best_header_index = i

    if best_header_index == -1:
        return 0, 1, df.iloc[0].astype(str).tolist() # Fallback

    data_start_index = best_header_index + 1
    
    # Construct the merged header
    primary_header = [str(h).replace('\n', ' ').strip() for h in df.iloc[best_header_index]]
    
    # Merge any info from rows above the primary header
    for i in range(start_offset, best_header_index):
        secondary_row = [str(h).replace('\n', ' ').strip() for h in df.iloc[i]]
        for j, text in enumerate(secondary_row):
            if text and text not in primary_header[j]:
                primary_header[j] = f"{text} {primary_header[j]}".strip()
                
    return best_header_index, data_start_index, primary_header

# --- Save to Excel (Main processing) ---

def process_and_save_to_excel(merged_tables, output_filename="master_output.xlsx"):
    """
    Processes extracted tables and saves them to a single master Excel file.
    """
    if not merged_tables:
        print("No tables to process for Excel."); return
    all_data = []
    print("\nProcessing tables for master Excel file...")
    for table_heading, df in merged_tables.items():
        if df.shape[1] < 2 or df.shape[0] < 2:
            print(f"INFO: Skipping table '{table_heading}' due to insufficient size."); continue
            
        # Use the advanced function to find the header and data
        header_row_index, data_start_index, new_header = find_header_and_data_start(df)
        
        df_data = df[data_start_index:].copy()
        
        if df_data.empty:
            print(f"INFO: Skipping table '{table_heading}' as no data rows found."); continue
        
        df_data.columns = new_header
        id_col_name = df_data.columns[0]
        
        try:
            if id_col_name == "" or pd.isna(id_col_name):
                print(f"WARNING: Skipping table '{table_heading}' due to empty identifier column."); continue
            df_melted = df_data.melt(id_vars=[id_col_name], var_name='ColumnHeader', value_name='Value')
        except Exception as e:
            print(f"WARNING: Skipping table '{table_heading}' during melt operation: {e}"); continue
            
        for _, row in df_melted.iterrows():
            value = row['Value']
            if pd.isna(value) or str(value).strip() in ['', '-']: continue
            table_heading_clean = str(table_heading).replace('\n', ' ').strip()
            row_category_clean = str(row[id_col_name]).replace('\n', ' ').strip()
            col_category_clean = str(row['ColumnHeader']).replace('\n', ' ').strip()
            header_tuple = (table_heading_clean, col_category_clean, row_category_clean)
            all_data.append({'Header': header_tuple, 'Value': value})
            
    if not all_data:
        print("No valid data extracted to write to Excel."); return
        
    final_df = pd.DataFrame(all_data)
    
    # --- <<< FIX: PROACTIVELY DE-DUPLICATE HEADERS >>> ---
    if final_df['Header'].duplicated().any():
        # Create a counter for each duplicated header
        counts = final_df.groupby('Header').cumcount()
        # Get the indices of the rows that are duplicates
        duplicated_indices = counts[counts > 0].index
        # Append a suffix to the last element of the tuple for each duplicate
        for idx in duplicated_indices:
            header_list = list(final_df.loc[idx, 'Header'])
            header_list[-1] = f"{header_list[-1]}_{counts[idx]}"
            final_df.loc[idx, 'Header'] = tuple(header_list)

    multi_index = pd.MultiIndex.from_tuples(final_df['Header'])
    wide_df = pd.DataFrame([final_df['Value'].values], columns=multi_index)

    try:
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            wide_df.to_excel(writer, index=True, sheet_name='Sheet1')
            worksheet = writer.sheets['Sheet1']
            for col_idx in range(1, worksheet.max_column + 1):
                col_letter = get_column_letter(col_idx)
                max_length = 0
                for cell in worksheet[col_letter]:
                    try:
                        if len(str(cell.value)) > max_length: max_length = len(str(cell.value))
                    except: pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[col_letter].width = adjusted_width
        print(f"\n✅ Successfully saved master data to '{output_filename}' with auto-fitted columns.")
    except Exception as e:
        print(f"\n❌ Error saving to master Excel file: {e}")

# --- Main ---

def main(debug=False):
    """Main function to run the PDF table extraction and processing."""
    pdf_path = "iit_delhi_data.pdf"
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found at '{pdf_path}'."); return
    
    print("\nStarting table extraction and grouping...")
    merged_tables = extract_and_group_tables(pdf_path, debug=debug)
    
    if merged_tables:
        save_raw_tables_for_debug(merged_tables)
        process_and_save_to_excel(merged_tables, output_filename="master_output.xlsx")

if __name__ == "__main__":
    main(debug=False)