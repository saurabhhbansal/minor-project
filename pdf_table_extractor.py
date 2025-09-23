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
            key = group_heading if group_heading else f"Table_Group_{i+1}Page{group[0].page}"
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

# --- ADVANCED HEADER ANALYSIS FUNCTION ---

def find_header_and_data_start(df: pd.DataFrame) -> Tuple[int, int, list]:
    """
    Analyzes the first few rows of a DataFrame to find the true header row,
    data start row, and constructs a merged, clean header.
    """
    def is_row_numerical_index(row):
        return all(str(item).strip() == str(i) for i, item in enumerate(row))
    
    def has_year_pattern(row):
        """Check if row contains year patterns like 2018-19, 2019-20, etc."""
        year_pattern = re.compile(r'\b20\d{2}-\d{2}\b')
        return any(year_pattern.search(str(cell)) for cell in row if pd.notna(cell))
    
    def count_meaningful_cells(row):
        """Count cells with meaningful content (not empty, not just numbers)"""
        meaningful = 0
        for cell in row:
            cell_str = str(cell).strip()
            if cell_str and cell_str != 'nan' and len(cell_str) > 1:
                # Check if it's not just a single number
                if not (cell_str.isdigit() and len(cell_str) <= 3):
                    meaningful += 1
        return meaningful
    
    start_offset = 0
    if len(df) > 0 and is_row_numerical_index(df.iloc[0]):
        start_offset = 1

    best_header_index = -1
    max_meaningful_cells = -1

    # Look through more rows to find the best header
    search_range = min(start_offset + 6, len(df))
    
    for i in range(start_offset, search_range):
        if i >= len(df):
            break
            
        row = df.iloc[i]
        meaningful_cells = count_meaningful_cells(row)
        
        # Skip rows that look like data (contain years)
        if has_year_pattern(row):
            continue
            
        # Prefer rows with more meaningful content
        if meaningful_cells > max_meaningful_cells:
            max_meaningful_cells = meaningful_cells
            best_header_index = i

    if best_header_index == -1:
        # Fallback to first non-year row
        for i in range(start_offset, min(start_offset + 4, len(df))):
            if i < len(df) and not has_year_pattern(df.iloc[i]):
                best_header_index = i
                break
        
        if best_header_index == -1:
            best_header_index = start_offset if start_offset < len(df) else 0

    # Find where data actually starts (first row with year pattern or after header)
    data_start_index = best_header_index + 1
    for i in range(best_header_index + 1, min(best_header_index + 4, len(df))):
        if i < len(df) and has_year_pattern(df.iloc[i]):
            data_start_index = i
            break
    
    # Construct the merged header
    if best_header_index < len(df):
        primary_header = [str(h).replace('\n', ' ').strip() for h in df.iloc[best_header_index]]
    else:
        primary_header = [f"Column_{i}" for i in range(len(df.columns))]
    
    # Merge any info from rows above the primary header
    for i in range(start_offset, best_header_index):
        if i < len(df):
            secondary_row = [str(h).replace('\n', ' ').strip() for h in df.iloc[i]]
            for j, text in enumerate(secondary_row):
                if j < len(primary_header) and text and text not in primary_header[j] and len(text) > 2:
                    if primary_header[j] and primary_header[j] != 'nan':
                        primary_header[j] = f"{text} {primary_header[j]}".strip()
                    else:
                        primary_header[j] = text
    
    # Clean up header names
    cleaned_header = []
    for header in primary_header:
        if header and header != 'nan' and header.strip():
            cleaned_header.append(header.strip())
        else:
            cleaned_header.append(f"Column_{len(cleaned_header)}")
                
    return best_header_index, data_start_index, cleaned_header

def process_pivot_table(df_data, headers, table_heading, debug=False):
    """
    Processes tables where Academic Year columns repeat, indicating grouped data structure.
    Example: [Academic Year, Data1, Data2, Academic Year, Data3, Academic Year, Data4, Data5]
    """
    processed_data = []
    
    if debug:
        print(f"Processing pivot table with headers: {headers}")
        print(f"Data shape: {df_data.shape}")
        print(f"First few rows:\n{df_data.head()}")
    
    # Find Academic Year column positions
    year_positions = []
    for i, header in enumerate(headers):
        if header == 'Academic Year' or header.startswith('Academic Year_'):
            year_positions.append(i)
    
    if debug:
        print(f"Academic Year positions: {year_positions}")
    
    # Add end position for easier processing
    year_positions.append(len(headers))
    
    # Process each row
    for row_idx, row in df_data.iterrows():
        if debug:
            print(f"\nProcessing row {row_idx}: {row.tolist()}")
        
        # Process each academic year group
        for i in range(len(year_positions) - 1):
            start_pos = year_positions[i]
            end_pos = year_positions[i + 1]
            
            # Get the academic year from the year column
            academic_year = str(row.iloc[start_pos]).strip()
            if not academic_year or academic_year == 'nan':
                if debug: print(f"  Skipping empty academic year at position {start_pos}")
                continue
            
            if debug:
                print(f"  Processing year group: '{academic_year}', columns {start_pos} to {end_pos}")
            
            # Process data columns for this academic year
            for col_idx in range(start_pos + 1, end_pos):
                if col_idx < len(headers) and col_idx < len(row):
                    column_header = headers[col_idx]
                    value = row.iloc[col_idx]
                    
                    if debug:
                        print(f"    Checking column {col_idx}: '{column_header}' = '{value}'")
                    
                    # Skip empty values
                    if pd.isna(value) or str(value).strip() in ['', '-', 'nan']:
                        if debug: print(f"      Skipping empty value")
                        continue
                    
                    # Create a meaningful row identifier from the first non-Academic Year column in the first group
                    if i == 0:  # Use first group for row identifier
                        # Find the first non-Academic Year column in the first group
                        first_data_col = year_positions[0] + 1
                        if first_data_col < year_positions[1] and first_data_col < len(row):
                            row_category = f"{headers[first_data_col]}_{row.iloc[first_data_col]}"
                        else:
                            row_category = f"Row_{row_idx}"
                    else:
                        row_category = f"Row_{row_idx}"
                    
                    # Create hierarchical header
                    table_heading_clean = str(table_heading).replace('\n', ' ').strip()
                    header_tuple = (table_heading_clean, f"{academic_year} - {column_header}", row_category)
                    processed_data.append({'Header': header_tuple, 'Value': str(value).strip()})
                    
                    if debug:
                        print(f"      Added: {header_tuple} = {value}")
    
    if debug:
        print(f"\nProcessed {len(processed_data)} data points from pivot table")
    
    return processed_data

# --- Save to Excel (Main processing) ---

def process_and_save_to_excel(merged_tables, output_filename="master_output.xlsx", debug=False):
    """
    Processes extracted tables and saves them to a single master Excel file.
    """
    if not merged_tables:
        print("No tables to process for Excel."); return
    all_data = []
    print("\nProcessing tables for master Excel file...")
    
    processed_count = 0
    skipped_count = 0
    
    for table_heading, df in merged_tables.items():
        if debug:
            print(f"\n=== Processing table: '{table_heading}' ===")
            print(f"Original shape: {df.shape}")
        
        try:
            # Basic validation
            if df.shape[1] < 2 or df.shape[0] < 2:
                if debug: print(f"INFO: Skipping table '{table_heading}' due to insufficient size.")
                skipped_count += 1
                continue
                
            # Use the advanced function to find the header and data
            header_row_index, data_start_index, new_header = find_header_and_data_start(df)
            
            if debug:
                print(f"Header analysis results:")
                print(f"  Header row index: {header_row_index}")
                print(f"  Data start index: {data_start_index}")
                print(f"  New header: {new_header}")
                print(f"  Original shape: {df.shape}")
                if header_row_index < len(df):
                    print(f"  Header row content: {df.iloc[header_row_index].tolist()}")
                if data_start_index < len(df):
                    print(f"  First data row: {df.iloc[data_start_index].tolist()}")
            
            # Check if table contains "Qualification" column and skip if it does
            if any("qualification" in str(header).lower() for header in new_header):
                if debug: print(f"INFO: Skipping table '{table_heading}' as it contains a 'Qualification' column.")
                skipped_count += 1
                continue
            
            if debug:
                print(f"Header row index: {header_row_index}")
                print(f"Data start index: {data_start_index}")
                print(f"New header length: {len(new_header)}")
                print(f"New header: {new_header}")
            
            # Validate data_start_index
            if data_start_index >= len(df):
                if debug: print(f"WARNING: data_start_index {data_start_index} >= df length {len(df)}. Skipping.")
                skipped_count += 1
                continue
            
            # Use iloc for safer integer-based indexing
            df_data = df.iloc[data_start_index:].copy()
            
            if debug:
                print(f"Data frame shape after slicing: {df_data.shape}")
            
            if df_data.empty:
                if debug: print(f"INFO: Skipping table '{table_heading}' as no data rows found.")
                skipped_count += 1
                continue
            
            # Validate header length matches columns
            if len(new_header) != len(df_data.columns):
                if debug: 
                    print(f"WARNING: Header length {len(new_header)} != columns length {len(df_data.columns)}")
                    print(f"Using original headers instead.")
                new_header = df_data.columns.tolist()
            
            # Handle duplicate column names
            seen_headers = {}
            clean_headers = []
            for header in new_header:
                if header in seen_headers:
                    seen_headers[header] += 1
                    clean_headers.append(f"{header}_{seen_headers[header]}")
                else:
                    seen_headers[header] = 0
                    clean_headers.append(header)
            
            df_data.columns = clean_headers
            
            # Handle tables with repeating Academic Year columns (pivot structure)
            if clean_headers.count('Academic Year') > 1 or any('Academic Year_' in h for h in clean_headers):
                if debug: print(f"Detected pivot table structure with multiple Academic Year columns")
                processed_data = process_pivot_table(df_data, clean_headers, table_heading, debug)
                all_data.extend(processed_data)
                processed_count += 1
                continue
            
            # Validate we have at least one column for id_vars
            if len(df_data.columns) == 0:
                if debug: print(f"WARNING: No columns in table '{table_heading}'. Skipping.")
                skipped_count += 1
                continue
            
            id_col_name = clean_headers[0]
            
            # Validate identifier column
            if id_col_name == "" or pd.isna(id_col_name):
                if debug: print(f"WARNING: Skipping table '{table_heading}' due to empty identifier column.")
                skipped_count += 1
                continue
            
            if debug:
                print(f"Identifier column: '{id_col_name}'")
                print(f"All columns: {df_data.columns.tolist()}")
                print(f"First few rows of data:")
                print(df_data.head(2))
            
            # Perform the melt operation
            try:
                df_melted = df_data.melt(id_vars=[id_col_name], var_name='ColumnHeader', value_name='Value')
                
                if debug:
                    print(f"Melted data shape: {df_melted.shape}")
                    print(f"Melted data sample:")
                    print(df_melted.head(3))
                
            except Exception as melt_error:
                if debug: 
                    print(f"ERROR during melt operation: {melt_error}")
                    print(f"DataFrame info:")
                    print(f"  Shape: {df_data.shape}")
                    print(f"  Columns: {df_data.columns.tolist()}")
                    print(f"  id_col_name: {id_col_name}")
                skipped_count += 1
                continue
            
            # Process each row in the melted data
            rows_processed = 0
            for _, row in df_melted.iterrows():
                value = row['Value']
                if pd.isna(value) or str(value).strip() in ['', '-']: 
                    continue
                
                table_heading_clean = str(table_heading).replace('\n', ' ').strip()
                row_category_clean = str(row[id_col_name]).replace('\n', ' ').strip()
                col_category_clean = str(row['ColumnHeader']).replace('\n', ' ').strip()
                header_tuple = (table_heading_clean, col_category_clean, row_category_clean)
                all_data.append({'Header': header_tuple, 'Value': value})
                rows_processed += 1
            
            if debug: print(f"Successfully processed {rows_processed} rows from table '{table_heading}'")
            processed_count += 1
            
        except Exception as e:
            if debug: 
                print(f"ERROR processing table '{table_heading}': {e}")
                import traceback
                traceback.print_exc()
            skipped_count += 1
            continue
    
    print(f"\nProcessing summary: {processed_count} tables processed, {skipped_count} tables skipped")
    
    if not all_data:
        print("No valid data extracted to write to Excel."); return
        
    final_df = pd.DataFrame(all_data)
    
    if debug:
        print(f"\nFinal data shape: {final_df.shape}")
        print(f"Sample of final data:")
        print(final_df.head(10))
    
    # Proactively de-duplicate headers
    if final_df['Header'].duplicated().any():
        if debug: print("Found duplicate headers, applying de-duplication...")
        # Create a counter for each duplicated header
        counts = final_df.groupby('Header').cumcount()
        # Get the indices of the rows that are duplicates
        duplicated_indices = counts[counts > 0].index
        # Append a suffix to the last element of the tuple for each duplicate
        for idx in duplicated_indices:
            header_list = list(final_df.loc[idx, 'Header'])
            header_list[-1] = f"{header_list[-1]}_{counts[idx]}"
            final_df.loc[idx, 'Header'] = tuple(header_list)
    
    # Create multi-index for wide format
    multi_index = pd.MultiIndex.from_tuples(final_df['Header'])
    wide_df = pd.DataFrame([final_df['Value'].values], columns=multi_index)
    
    if debug:
        print(f"\nWide DataFrame shape: {wide_df.shape}")
        print(f"Wide DataFrame columns: {len(wide_df.columns)}")

    try:
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            wide_df.to_excel(writer, index=True, sheet_name='Sheet1')
            worksheet = writer.sheets['Sheet1']
            
            # Auto-fit columns
            for col_idx in range(1, worksheet.max_column + 1):
                col_letter = get_column_letter(col_idx)
                max_length = 0
                for cell in worksheet[col_letter]:
                    try:
                        if len(str(cell.value)) > max_length: 
                            max_length = len(str(cell.value))
                    except: 
                        pass
                adjusted_width = min((max_length + 2), 50)  # Cap at 50 characters
                worksheet.column_dimensions[col_letter].width = adjusted_width
        
        print(f"\n✅ Successfully saved master data to '{output_filename}'")
        print(f"✅ Total columns: {len(wide_df.columns)}")
        print(f"✅ File size: {os.path.getsize(output_filename) / 1024:.1f} KB")
        
    except Exception as e:
        print(f"\n❌ Error saving to master Excel file: {e}")
        import traceback
        traceback.print_exc()

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
        process_and_save_to_excel(merged_tables, output_filename="master_output.xlsx", debug=debug)
    else:
        print("No tables were extracted from the PDF.")

if __name__ == "__main__":
    main(debug=True)  # Set to False for less verbose output with extreme debugging