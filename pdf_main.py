"""
PDF Table Extractor with a unified processing engine.
This script is designed to be called by a master script and process one folder at a time.
"""

import camelot
import pandas as pd
import os
import re
import fitz  # PyMuPDF
import sys
from typing import Optional, Tuple, List, Dict
from openpyxl.utils import get_column_letter

# --- Helper Functions ---

def _sanitize_text(text: str) -> str:
    """Cleans up text to be used as a filename or key."""
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    return text[:160]

def _normalize_identifier_text(text: str) -> str:
    """Standardizes specific row identifier text for consistent column mapping."""
    text = str(text).strip()
    if text.startswith("Library"): return "Library"
    if text.startswith("New Equipment"): return "New Equipment for Laboratories"
    if text.startswith("Other expenditure on creation of Capital Assets"): return "Other expenditure on creation of Capital Assets (excluding expenditure on Land and Building)"
    return text

def _is_faculty_token(s: str) -> bool:
    """Returns True if the string looks like a faculty-related field."""
    s = str(s or "").lower()
    tokens = [
        'srno', 'sr.no', 's.no', 'serial no', 'sno', 'sr no', '#',
        'name', 'age', 'gender', 'designation', 'qualification',
        'experience', 'experience (in months)', 'currently working',
        'joining date', 'date of joining', 'leaving date', 'date of leaving',
        'association', 'association type', 'department', 'employee id', 'emp id',
        'faculty id'
    ]
    return any(tok in s for tok in tokens)

def _is_faculty_header_tuple(header_tuple: Tuple) -> bool:
    """Heuristic: decide if a header tuple clearly refers to faculty roster columns."""
    parts = [str(p) for p in header_tuple if isinstance(p, (str,))]
    lower_parts = [p.lower() for p in parts]

    def looks_like_date(v: str) -> bool:
        return bool(re.search(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b", v))

    def looks_like_small_int(v: str) -> bool:
        return v.isdigit() and 1 <= int(v) <= 200

    def looks_like_name(v: str) -> bool:
        if any(ch.isdigit() for ch in v):
            return False
        tokens = [t for t in re.split(r"\s+", v.strip()) if t]
        if len(tokens) < 2 or len(tokens) > 4:
            return False
        # Allow tokens like 'Abdul', 'Khan', 'Ishrat', 'Bashir', 'Asif'
        title_like = sum(1 for t in tokens if t[:1].isalpha() and t[:1].isupper())
        return title_like >= 2

    def is_status_word(v: str) -> bool:
        v = v.lower().strip()
        statuses = ['professor', 'assistant professor', 'associate professor', 'regular', 'contract', 'adhoc', 'visiting', 'temporary', 'permanent']
        return any(s in v for s in statuses)

    # Token-based hits from known faculty column names
    hits = sum(1 for p in lower_parts if _is_faculty_token(p))
    if hits >= 2:
        return True

    # Value-pattern based heuristics catching cases like:
    # ('Professor', '19'), ('Abdul Gani', '22'), ('30-07-2013', '23'), ('Regular', '33')
    if any(is_status_word(p) for p in parts):
        return True
    if any(looks_like_date(p) for p in parts) and (any(looks_like_name(p) for p in parts) or any(looks_like_small_int(p) for p in parts)):
        return True
    if any(looks_like_name(p) for p in parts) and (any(looks_like_small_int(p) for p in parts) or any(is_status_word(p) for p in parts)):
        return True

    return False

def _to_fitz_y(page, camelot_y: float) -> float:
    return float(page.rect.height) - float(camelot_y)

def _horizontal_overlap(bbox1, bbox2):
    x1_l, _, x1_r, _ = bbox1
    x2_l, _, x2_r, _ = bbox2
    if x1_r < x2_l or x2_r < x1_l: return 0
    intersection = min(x1_r, x2_r) - max(x1_l, x2_l)
    min_width = min(x1_r - x1_l, x2_r - x2_l)
    return intersection / min_width if min_width > 0 else 0

def _find_heading_above(page, bbox, band_px: int = 120) -> Optional[str]:
    try: _, _, _, y_top = bbox
    except (TypeError, ValueError): return None
    table_top_fitz = _to_fitz_y(page, y_top)
    blocks = page.get_text("blocks") or []
    candidates = []
    for b in blocks:
        try: _, _, _, _, text, _, _ = b
        except ValueError: continue
        if text.strip() and (table_top_fitz - b[3]) <= band_px and b[3] <= table_top_fitz:
            s = text.strip()
            if 2 <= len(s) <= 140 and sum(c.isalpha() for c in s) > 0 and (sum(c.isdigit() for c in s) / len(s)) <= 0.4:
                candidates.append((table_top_fitz - b[3], s))
    if not candidates: return None
    return _sanitize_text(min(candidates, key=lambda t: t[0])[1])

def extract_and_group_tables(pdf_path, pages='all', debug=False):
    try:
        if not os.path.exists(pdf_path): raise FileNotFoundError(f"PDF not found: {pdf_path}")
        print(f"Extracting tables from: {pdf_path}...")
        tables = camelot.read_pdf(pdf_path, pages=pages, flavor='lattice', line_scale=40)
        if not tables: print("No tables found."); return {}
        print(f"Found {len(tables)} initial tables. Filtering and grouping...")
        filtered = [t for t in tables if hasattr(t, 'df') and isinstance(t.df, pd.DataFrame) and not t.df.empty]
        def safe_bbox(t): return getattr(t, "_bbox", t.parsing_report.get("bbox", (0,0,0,0)))
        sorted_tables = sorted(filtered, key=lambda t: (int(t.page), -safe_bbox(t)[3]))
        groups, doc = [], fitz.open(pdf_path)
        for t in sorted_tables:
            page = doc[int(t.page) - 1]
            t.heading = _find_heading_above(page, safe_bbox(t))
            if groups and not t.heading and int(t.page) == int(groups[-1][-1].page) + 1 and _horizontal_overlap(safe_bbox(t), safe_bbox(groups[-1][-1])) > 0.5:
                groups[-1].append(t)
            else:
                groups.append([t])
        final_tables = {}
        for i, group in enumerate(groups):
            key = group[0].heading or f"Table_Group_{i+1}_Page{group[0].page}"
            original_key, counter = key, 1
            while key in final_tables: key = f"{original_key}_{counter}"; counter += 1
            final_tables[key] = pd.concat([t.df for t in group], ignore_index=True)
        doc.close()
        for h, d in final_tables.items(): print(f"\nGroup: '{h}' -> Rows: {len(d)}, Cols: {len(d.columns)}")
        return final_tables
    except Exception as e:
        print(f"Error in extract_and_group_tables: {e}"); return {}

def find_header_and_data_start(df: pd.DataFrame) -> Tuple[int, int, list]:
    def is_num_index(row): return all(str(item).strip() == str(i) for i, item in enumerate(row))
    def has_year(row): return any(re.search(r'\b20\d{2}-\d{2}\b', str(c)) for c in row if pd.notna(c))
    def is_header(row):
        # CORRECTED: Added more keywords to reliably identify faculty table headers
        keywords = ['year', 'program', 'students', 'total', 'no.', 'amount', 'designation', 'qualification', 'srno', 'name']
        return any(any(kw in str(c).lower() for kw in keywords) for c in row)
    start_offset = 1 if len(df) > 0 and is_num_index(df.iloc[0]) else 0
    best_idx = -1
    search_range = min(start_offset + 5, len(df))
    for i in range(start_offset, search_range):
        row = df.iloc[i]
        if has_year(row) and not is_header(row): continue
        if is_header(row): best_idx = i; break
    if best_idx == -1:
        max_meaning, best_idx = -1, start_offset
        for i in range(start_offset, search_range):
            meaningful = sum(1 for c in df.iloc[i] if pd.notna(c) and isinstance(c, str) and len(c.strip()) > 3)
            if meaningful > max_meaning: max_meaning, best_idx = meaningful, i
    if best_idx == -1: best_idx = start_offset
    header = [str(h).replace('\n',' ').strip() for h in df.iloc[best_idx]] if best_idx < len(df) else [f"C{i}" for i in range(df.shape[1])]
    # Do not create placeholder columns with the name 'Col_*'; use 'Field_*' instead (1-based index)
    cleaned = [h if h and h.lower() != 'nan' else f"Field_{i+1}" for i, h in enumerate(header)]
    return best_idx, best_idx + 1, cleaned

# --- Special Table Handlers ---

def hardcoded_phd_parser(df, table_heading, all_data, debug=False):
    if debug: print("Applying HARDCODED Ph.D./PG parser...")
    df_str = df.astype(str)
    data_found = False
    try: # Ph.D Pursuing
        pursuing_row_idx, pursuing_heading = -1, ""
        for r_idx in range(len(df_str)):
            if 'ph.d (student pursuing' in df_str.iloc[r_idx].to_string().lower():
                pursuing_heading = df_str.iloc[r_idx, 0].strip()
                if pursuing_heading.startswith("Ph.D (Student pursuing doctoral program"):
                    pursuing_heading = "Ph.D (Student pursuing doctoral program till 2023-24)"
                pursuing_row_idx = r_idx; break
        if pursuing_row_idx != -1:
            ts_col_idx, header_row_idx = -1, -1
            for r_offset in range(3):
                if pursuing_row_idx + r_offset >= len(df_str): break
                row = df_str.iloc[pursuing_row_idx + r_offset]
                for c_idx, cell in enumerate(row):
                    if 'total students' in str(cell).lower():
                        ts_col_idx, header_row_idx = c_idx, pursuing_row_idx + r_offset; break
                if ts_col_idx != -1: break
            if ts_col_idx != -1:
                for r_idx in range(header_row_idx + 1, len(df_str)):
                    row_label = df_str.iloc[r_idx, 0].strip().lower()
                    if row_label in ['full time', 'part time']:
                        value = str(df_str.iloc[r_idx, ts_col_idx]).split('\n')[-1].strip()
                        if value and value.lower() not in ['nan', '']:
                            all_data.append({'Header': (table_heading, pursuing_heading, row_label.title()), 'Value': value}); data_found = True
                    elif 'graduated' in df_str.iloc[r_idx].to_string().lower() or 'pg (student' in df_str.iloc[r_idx].to_string().lower():
                        break
    except Exception as e:
        if debug: print(f"  - Error in Ph.D. Pursuing section: {e}")
    try: # Ph.D Graduated
        grad_row_idx, grad_heading = -1, ""
        for r_idx in range(len(df_str)):
            if 'students graduated' in df_str.iloc[r_idx].to_string().lower() and 'ph.d' in df_str.iloc[r_idx].to_string().lower():
                grad_heading, grad_row_idx = df_str.iloc[r_idx, 0].strip(), r_idx; break
        if grad_row_idx != -1:
            year_headers, year_row_idx = [], -1
            for r_offset in range(1, 4):
                if grad_row_idx + r_offset >= len(df_str): break
                row = df_str.iloc[grad_row_idx + r_offset]
                if sum(1 for c in row if re.search(r'20\d{2}-\d{2}', str(c))) >= 1:
                    year_headers, year_row_idx = row.tolist(), grad_row_idx + r_offset; break
            if year_row_idx != -1:
                for r_idx in range(year_row_idx + 1, len(df_str)):
                    row_label = df_str.iloc[r_idx, 0].strip().lower()
                    if row_label in ['full time', 'part time']:
                        for c_idx in range(1, len(df_str.columns)):
                            if c_idx < len(year_headers) and re.search(r'20\d{2}-\d{2}', str(year_headers[c_idx])):
                                year, value = str(year_headers[c_idx]).strip(), str(df_str.iloc[r_idx, c_idx]).strip()
                                if value and value.lower() not in ['nan', '']:
                                    all_data.append({'Header': (table_heading, f"{grad_heading} {year}", row_label.title()), 'Value': value}); data_found = True
                    elif 'pg (student' in df_str.iloc[r_idx].to_string().lower():
                        break
    except Exception as e:
        if debug: print(f"  - Error in Ph.D. Graduated section: {e}")
    return data_found

def process_pivot_table(df_data, headers, table_heading, debug=False):
    if debug: print(f"Pivot normalization. Headers: {headers}")
    year_pos = [i for i, h in enumerate(headers) if 'academic year' in h.lower()]
    if not year_pos: return []
    year_pos.append(len(headers))
    records = []
    for i in range(len(year_pos) - 1):
        year_col, metric_cols = year_pos[i], range(year_pos[i] + 1, year_pos[i+1])
        for _, row in df_data.iterrows():
            year = str(row.iloc[year_col]).strip()
            if not year or not re.search(r'\d', year) or 'academic' in year.lower():
                continue
            for mc in metric_cols:
                if mc >= len(headers) or mc >= len(row): continue
                metric = headers[mc]
                if "Median salary" in metric: metric = "Median salary of placed graduates(Amount in Rs.)"
                val = 0 if str(row.iloc[mc]).strip() == '0' else None if pd.isna(row.iloc[mc]) or str(row.iloc[mc]).strip().lower() in ['', 'nan'] else str(row.iloc[mc]).strip()
                records.append({'Header': (table_heading, metric, year), 'Value': val})
    return records

# --- Unified Data Processing Engine ---

def _generate_records_from_df(df: pd.DataFrame, table_heading: str, debug: bool = False) -> List[Dict]:
    """Single, unified engine to process a dataframe and return standardized records."""
    records = []
    try:
        if df.empty: return []
        
        # EARLY CHECK: Skip faculty tables by examining raw dataframe content
        df_as_string = df.to_string().lower()

        # Expanded indicators to robustly catch faculty rosters across PDFs
        faculty_indicators = [
            'faculty', 'designation', 'qualification', 'experience', 'experience (in months)',
            'currently working', 'joining date', 'date of joining', 'leaving date', 'date of leaving',
            'association', 'association type', 'department', 'employee id', 'emp id', 'faculty id',
            'email', 'phone', 'mobile'
        ]
        faculty_match_count = sum(1 for indicator in faculty_indicators if indicator in df_as_string)

        # If this table strongly resembles faculty data, skip early
        if faculty_match_count >= 3 or (
            faculty_match_count >= 2 and df.shape[0] >= 15 and df.shape[1] >= 4
        ) or (
            'faculty' in df_as_string and any(k in df_as_string for k in ['designation', 'qualification', 'joining'])
        ):
            print(f"⏭️  Skipping Faculty Details table (early content check): '{table_heading}' [matches={faculty_match_count}]")
            return records
        
        if 'ph.d (student pursuing' in df_as_string or 'students graduated' in df_as_string:
            if debug: print("Ph.D. table signature found, routing to hardcoded parser.")
            if hardcoded_phd_parser(df, table_heading, records, debug): return records
        if df.shape[1] == 1:
            all_rows, current_q, current_a = df.iloc[:, 0].dropna().astype(str).tolist(), None, []
            def save():
                if current_q and current_a: records.append({'Header': (table_heading, current_q.strip(), ''), 'Value': ", ".join(a.strip() for a in current_a)})
            for row_text in all_rows:
                cleaned = row_text.replace('\n', ' ').strip()
                if not cleaned: continue
                if re.match(r'^\d+\.\s+', cleaned): save(); current_q, current_a = cleaned, []
                elif current_q:
                    answer = re.sub(r'^[•-]\s', '', cleaned).strip()
                    if answer: current_a.append(answer)
            save(); return records
        _, data_start_index, new_header = find_header_and_data_start(df)
        
        first_col_content = df.iloc[:, 0].to_string().lower()
        if ('sponsored projects' in first_col_content or 'client organizations' in first_col_content) and any('year' in h.lower() for h in new_header):
            is_consultancy = 'client organizations' in first_col_content
            df_timeline = df.iloc[data_start_index:].copy()
            df_timeline.columns = new_header
            id_col = new_header[0]
            if is_consultancy:
                df_timeline[id_col] = df_timeline[id_col].str.replace("Total Amount Received (Amount in Rupees)", "Total Amount Received (Amount in Rupees) CP", regex=False)
                df_timeline[id_col] = df_timeline[id_col].str.replace("Amount Received in Words", "Amount Received in Words CP", regex=False)
            for _, row in df_timeline.melt(id_vars=[id_col], var_name='Year', value_name='Value').iterrows():
                val = row['Value']; cleaned = 0 if str(val).strip() == '0' else None if pd.isna(val) or str(val).strip().lower() in ['', 'nan'] else str(val).strip()
                records.append({'Header': (table_heading, str(row[id_col]).strip(), str(row['Year']).strip()), 'Value': cleaned})
            return records
        if data_start_index >= len(df) or df.iloc[data_start_index:].empty: return []
        df_data = df.iloc[data_start_index:].copy()
        
    # This is the single, definitive check to skip faculty tables
    # Check for faculty table patterns using the detected headers
        header_text = ' '.join([str(h).lower() for h in new_header])
        
        # Multiple detection strategies to catch all faculty table variations
        has_qualification = any("qualification" in str(h).lower() for h in new_header)
        has_designation = any("designation" in str(h).lower() for h in new_header)
        has_name = any("name" in str(h).lower() for h in new_header)
        has_srno = any(
            s in str(h).lower() for h in new_header for s in ["srno", "sr.no", "s.no", "s no", "serial no", "sno", "sr no", "#", "field_1"]
        )
        has_age = any("age" in str(h).lower() for h in new_header)
        has_gender = any("gender" in str(h).lower() for h in new_header)
        has_experience = any("experience" in str(h).lower() for h in new_header)
        has_joining = any("joining" in str(h).lower() for h in new_header)
        
        # Also check table heading for "faculty" or "months) currently working" patterns
        table_heading_lower = str(table_heading).lower()
        is_faculty_heading = "faculty" in table_heading_lower or "currently working" in table_heading_lower or "months)" in table_heading_lower
        
        # Check if table has many rows (>50) with Name-like data - typical faculty roster pattern
        is_large_roster = len(df_data) > 50 and has_name
        
        # Faculty table if it has typical faculty roster columns
        is_faculty_roster = (
            (has_qualification and has_designation)
            or (has_name and has_srno)
            or (has_name and has_designation)
            or (has_name and has_age and has_gender)
            or (has_experience and has_joining)
            or (is_faculty_heading and (has_name or has_designation or has_qualification))
            or is_large_roster
        )
        
        # Extra guard: if majority of headers look faculty-like, skip
        faculty_header_hits = sum(1 for h in new_header if _is_faculty_token(h))
        majority_faculty = faculty_header_hits >= max(3, int(0.5 * len(new_header)))

        if is_faculty_roster or majority_faculty:
            print(f"⏭️  Skipping Faculty Details table: '{table_heading}' (headers: {new_header[:5]}...)")
            return records
            
        if len(new_header) != len(df_data.columns): new_header = df_data.columns.tolist()
        seen, clean_headers = {}, []; [seen.setdefault(h,0) or clean_headers.append(h) if h not in seen else seen.update({h:seen[h]+1}) or clean_headers.append(f"{h}_{seen[h]}") for h in new_header]
        df_data.columns = clean_headers
        if len(df_data.columns) == 2:
            key_col, val_col = clean_headers
            for _, r in df_data.iterrows():
                key = str(r[key_col]).strip()
                if not key or key.lower() in ['nan']: continue
                val = r[val_col]; cleaned = 0 if str(val).strip() == '0' else None if pd.isna(val) or str(val).strip().lower() in ['', 'nan'] else str(val).strip()
                records.append({'Header': (table_heading, key, ''), 'Value': cleaned})
            return records
        if clean_headers.count('Academic Year') > 1 or any('Academic Year_' in h for h in clean_headers):
            return process_pivot_table(df_data, clean_headers, table_heading, debug)
        if not clean_headers: return []
        id_col = clean_headers[0]
        df_data[id_col] = df_data[id_col].apply(_normalize_identifier_text)
        for _, row in df_data.melt(id_vars=[id_col], var_name='ColHeader', value_name='Value').iterrows():
            val = row['Value']; cleaned = 0 if str(val).strip() == '0' else None if pd.isna(val) or str(val).strip().lower() in ['', 'nan'] else str(val).strip()
            records.append({'Header': (table_heading, str(row['ColHeader']).strip(), str(row[id_col]).strip()), 'Value': cleaned})
        return records
    except Exception as e:
        if debug: print(f"ERROR in _generate_records_from_df for table '{table_heading}': {e}")
        return []

# --- High-Level Functions (Single File, Folder, etc.) ---

def _normalize_header_drop_table(header_tuple: Tuple) -> Tuple[str, str]:
    if not header_tuple: return ("Unknown", "")
    parts = list(header_tuple)[1:]
    if not parts: return (str(header_tuple[0]), "")
    return (parts[0], " ".join(parts[1:])) if len(parts) > 1 else (parts[0], "")

def process_folder(input_folder: str, output_folder: str, debug: bool=False):
    pdf_files = sorted([f for f in os.listdir(input_folder) if f.lower().endswith('.pdf')])
    if not pdf_files: print(f"No PDF files found in '{input_folder}'."); return
    
    print(f"Found {len(pdf_files)} PDF(s). Aggregating...")
    global_columns, row_data = [], []
    for pdf_name in pdf_files:
        print(f"\n--- Processing PDF: {pdf_name} ---")
        all_records, normalized_map = [], {}
        merged_tables = extract_and_group_tables(os.path.join(input_folder, pdf_name), debug=debug)
        for table_heading, df in merged_tables.items():
            all_records.extend(_generate_records_from_df(df, table_heading, debug))

        for rec in all_records:
            if rec.get('Value') is None: continue
            norm = _normalize_header_drop_table(rec['Header'])
            # Guard: do not include any faculty-like columns in aggregation
            if _is_faculty_header_tuple(norm):
                continue
            if norm not in normalized_map: normalized_map[norm] = rec['Value']
            if norm not in global_columns: global_columns.append(norm)
        row_data.append({'_pdf': pdf_name, 'map_': normalized_map})

    matrix, idx = [], []
    for rd in row_data:
        # row_data was appended as {'_pdf': pdf_name, 'map_': normalized_map}
        matrix.append([rd['map_'].get(col, "NULL") for col in global_columns])
        idx.append(rd['_pdf'])
    
    wide_df = pd.DataFrame(matrix, columns=pd.MultiIndex.from_tuples(global_columns), index=idx)
    cols_to_drop = [col for col in wide_df.columns if col[1] == 'Academic Year' or col[0] == col[1]]
    # Final safety: drop any columns that match faculty roster patterns
    faculty_like_cols = [col for col in wide_df.columns if _is_faculty_header_tuple(col)]
    if faculty_like_cols:
        if debug:
            print(f"Dropping {len(faculty_like_cols)} faculty-like column(s) at aggregation stage.")
        wide_df = wide_df.drop(columns=faculty_like_cols, errors='ignore')
    if cols_to_drop:
        if debug: print(f"Cleaning up {len(cols_to_drop)} irrelevant columns...")
        wide_df = wide_df.drop(columns=cols_to_drop, errors='ignore')
    
    new_columns = [f"{h}:{s}" if s else h for h, s in wide_df.columns]
    wide_df.columns = new_columns

    folder_name = os.path.basename(os.path.normpath(input_folder))
    output_filename = os.path.join(output_folder, f"{folder_name}_master_output.xlsx")
    try:
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            wide_df.to_excel(writer, sheet_name='Aggregated_Data')
            ws = writer.sheets['Aggregated_Data']
            for col_idx, col in enumerate(ws.columns, 1):
                max_len = max((len(str(c.value)) for c in col if c.value), default=0)
                ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 2, 60)
        print(f"\n✅ Aggregated {len(pdf_files)} PDFs into '{output_filename}'")
    except Exception as e:
        print(f"Failed to write aggregated Excel: {e}")

# --- Main ---

def main():
    """
    This script is designed to be called with a command-line argument.
    It processes a single folder passed to it.
    """
    if len(sys.argv) != 3:
        print("Usage: python pdf_main.py <path_to_input_folder> <path_to_output_folder>")
        sys.exit(1)
    
    input_folder = sys.argv[1]
    output_folder = sys.argv[2]
    
    if os.path.isdir(input_folder):
        print(f"\nRunning in FOLDER mode for directory: '{input_folder}'")
        process_folder(input_folder, output_folder, debug=False)
    else:
        print(f"Error: Path '{input_folder}' is not a valid directory.")

if __name__ == "__main__":
    main()