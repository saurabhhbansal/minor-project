# app/services/pdf_extractor_backend.py
"""
Backend wrapper of your full working extractor.
- Keeps your exact extraction, header-analysis and mapping logic.
- Provides a single function `extract_financial_data(pdf_path)` that returns
  a 1-row wide DataFrame with the standard 25 NIRF financial columns.
- Does NOT write Excel or CSV files or upload to Supabase.
"""

import os
import re
import camelot
import pandas as pd
import fitz  # PyMuPDF
from typing import Optional, Tuple, List, Dict
from openpyxl.utils import get_column_letter
from datetime import datetime


# --- Standard CSV columns (25 columns used previously) ---
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

# --- Helper utilities reused from your working script ---
def _sanitize_text(text: str) -> str:
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    return text[:160]

def _to_fitz_y(page, camelot_y: float) -> float:
    return float(page.rect.height) - float(camelot_y)

def _horizontal_overlap(bbox1, bbox2):
    x1_l, _, x1_r, _ = bbox1
    x2_l, _, x2_r, _ = bbox2
    if x1_r < x2_l or x2_r < x1_l:
        return 0
    intersection_width = min(x1_r, x2_r) - max(x1_l, x2_l)
    width1 = x1_r - x1_l
    width2 = x2_r - x2_l
    return intersection_width / min(width1, width2) if min(width1, width2) > 0 else 0

def _find_heading_above(page, bbox, band_px: int = 120) -> Optional[str]:
    try:
        _, _, _, y_top = bbox
    except (TypeError, ValueError):
        return None
    table_top_fitz = _to_fitz_y(page, y_top)
    blocks = page.get_text("blocks") or []
    candidates = []
    for b in blocks:
        try:
            x0, y0, x1, y1, text, *_ = b
        except ValueError:
            continue
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

def _remove_bracket_content(text: str) -> str:
    return re.sub(r"\[.*?\]|\(.*?\)|\{.*?\}", "", text).strip()

# --- Core extraction & grouping (Camelot + PyMuPDF heading detection) ---
def extract_and_group_tables(pdf_path, pages='all', min_rows=1, min_cols=1):
    """
    Runs camelot.read_pdf(..., flavor='lattice') then groups tables that span pages
    or are part of the same logical block. Returns {group_heading_or_fallback: DataFrame}
    """
    try:
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        print(f"Extracting tables from: {pdf_path} using Camelot...")
        tables = camelot.read_pdf(pdf_path, pages=pages, flavor='lattice', line_scale=40)
        if not tables:
            print("No tables found by Camelot.")
            return {}
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
            if groups:
                last_group = groups[-1]
                last_table_in_group = last_group[-1]
                last_page = int(last_table_in_group.page)
                if heading:
                    groups.append([t])
                else:
                    overlap = _horizontal_overlap(bbox, safe_bbox(last_table_in_group))
                    if current_page == last_page + 1 and overlap > 0.5:
                        last_group.append(t)
                    else:
                        groups.append([t])
            else:
                groups.append([t])

        final_tables = {}
        for i, group in enumerate(groups):
            merged_df = pd.concat([t.df for t in group], ignore_index=True)
            group_heading = group[0].heading
            key = group_heading if group_heading else f"Table_Group_{i+1}Page{group[0].page}"
            original_key, counter = key, 1
            while key in final_tables:
                key = f"{original_key}_{counter}"
                counter += 1
            final_tables[key] = merged_df

        doc.close()
        print("\nFinal Groups Found:")
        for h, d in final_tables.items():
            print(f"  - Group: '{h}' -> Rows: {len(d)}, Cols: {len(d.columns)}")
        return final_tables
    except Exception as e:
        print(f"Error extracting tables: {str(e)}")
        return {}

# --- Header-finding and pivot helpers (copied exactly) ---
def find_header_and_data_start(df: pd.DataFrame) -> Tuple[int, int, list]:
    def is_row_numerical_index(row):
        return all(str(item).strip() == str(i) for i, item in enumerate(row))

    def has_year_pattern(row):
        year_pattern = re.compile(r'\b20\d{2}-\d{2}\b')
        return any(year_pattern.search(str(cell)) for cell in row if pd.notna(cell))

    def count_meaningful_cells(row):
        meaningful = 0
        for cell in row:
            cell_str = str(cell).strip()
            if cell_str and cell_str != 'nan' and len(cell_str) > 1:
                if not (cell_str.isdigit() and len(cell_str) <= 3):
                    meaningful += 1
        return meaningful

    def is_header_row(row):
        row_str = [str(cell).strip().lower() for cell in row]
        header_keywords = ['academic year', 'program', 'students', 'year', 'total', 'no.', 'amount']
        return any(any(keyword in cell for keyword in header_keywords) for cell in row_str)

    start_offset = 0
    if len(df) > 0 and is_row_numerical_index(df.iloc[0]):
        start_offset = 1

    best_header_index = -1
    search_range = min(start_offset + 4, len(df))
    for i in range(start_offset, search_range):
        if i >= len(df):
            break
        row = df.iloc[i]
        if has_year_pattern(row) and not is_header_row(row):
            continue
        if is_header_row(row):
            best_header_index = i
            break

    if best_header_index == -1:
        max_meaningful_cells = -1
        for i in range(start_offset, search_range):
            if i >= len(df):
                break
            row = df.iloc[i]
            meaningful_cells = count_meaningful_cells(row)
            if has_year_pattern(row):
                continue
            if meaningful_cells > max_meaningful_cells:
                max_meaningful_cells = meaningful_cells
                best_header_index = i

    if best_header_index == -1:
        best_header_index = start_offset if start_offset < len(df) else 0

    data_start_index = best_header_index + 1

    if best_header_index < len(df):
        primary_header = [str(h).replace('\n', ' ').strip() for h in df.iloc[best_header_index]]
    else:
        primary_header = [f"Column_{i}" for i in range(len(df.columns))]

    should_merge = False
    if best_header_index > start_offset:
        empty_or_short = sum(1 for h in primary_header if not h or h == 'nan' or len(h.strip()) < 2)
        if empty_or_short > len(primary_header) * 0.3:
            should_merge = True

    if should_merge:
        for i in range(start_offset, best_header_index):
            if i < len(df):
                secondary_row = [str(h).replace('\n', ' ').strip() for h in df.iloc[i]]
                for j, text in enumerate(secondary_row):
                    if j < len(primary_header) and text and text not in primary_header[j] and len(text) > 2:
                        if primary_header[j] and primary_header[j] != 'nan':
                            primary_header[j] = f"{text} {primary_header[j]}".strip()
                        else:
                            primary_header[j] = text

    cleaned_header = []
    for header in primary_header:
        if header and header != 'nan' and header.strip():
            cleaned_header.append(header.strip())
        else:
            cleaned_header.append(f"Column_{len(cleaned_header)}")

    return best_header_index, data_start_index, cleaned_header

def process_pivot_table(df_data, headers, table_heading):
    year_positions = [i for i, h in enumerate(headers) if h == 'Academic Year' or h.startswith('Academic Year_')]
    if not year_positions:
        return []
    year_positions_sorted = sorted(year_positions)
    year_positions_sorted.append(len(headers))
    table_heading_clean = str(table_heading).replace('\n', ' ').strip()
    processed_data: List[Dict] = []
    for idx in range(len(year_positions_sorted) - 1):
        year_col = year_positions_sorted[idx]
        next_boundary = year_positions_sorted[idx + 1]
        metric_cols = [c for c in range(year_col + 1, next_boundary) if c < len(headers)]
        for row_i, row in df_data.iterrows():
            if year_col >= len(row):
                continue
            year_raw = str(row.iloc[year_col]).strip()
            if not year_raw or year_raw.lower() in ['nan', '-']:
                continue
            for mc in metric_cols:
                if mc >= len(row):
                    continue
                metric_name = headers[mc]
                value_raw = row.iloc[mc]
                val_str = str(value_raw).strip()
                if val_str == '0':
                    cleaned_val = 0
                elif pd.isna(value_raw) or val_str.lower() in ['', '-', 'nan']:
                    cleaned_val = None
                else:
                    cleaned_val = val_str
                header_tuple = (table_heading_clean, metric_name, year_raw)
                processed_data.append({'Header': header_tuple, 'Value': cleaned_val})
    return processed_data

def process_combined_phd_table(df_data: pd.DataFrame, table_heading: str, all_data: list):
    year_pattern = re.compile(r'20\d{2}-\d{2}')
    total_students_pos = None
    for r_idx in range(len(df_data)):
        for c_idx in range(len(df_data.columns)):
            cell = str(df_data.iat[r_idx, c_idx]).strip()
            if cell.lower() == 'total students':
                total_students_pos = (r_idx, c_idx)
                break
        if total_students_pos:
            break
    if not total_students_pos:
        return False
    ts_row, ts_col = total_students_pos
    year_header_row = None
    for r_idx in range(ts_row + 1, len(df_data)):
        row = df_data.iloc[r_idx]
        year_hits, non_empty = 0, 0
        for c_idx in range(1, len(df_data.columns)):
            val = str(row.iloc[c_idx]).strip()
            if val and val.lower() not in ['nan', '-', '']:
                non_empty += 1
                if year_pattern.fullmatch(val):
                    year_hits += 1
        if year_hits >= 2 and year_hits >= max(1, non_empty - year_hits):
            year_header_row = r_idx
            break
    if not year_header_row:
        return False
    table_heading_clean = str(table_heading).replace('\n', ' ').strip()
    # Subtable 1
    for r_idx in range(ts_row + 1, year_header_row):
        category = str(df_data.iat[r_idx, 0]).replace('\n', ' ').strip()
        if not category or category.lower() in ['nan', '-', '']:
            continue
        raw_val = str(df_data.iat[r_idx, ts_col]).strip() if ts_col < len(df_data.columns) else ''
        if raw_val == '0':
            cleaned_val = 0
        elif raw_val.lower() in ['', '-', 'nan']:
            cleaned_val = None
        else:
            cleaned_val = raw_val
        header_tuple = (table_heading_clean, 'Total Students', category)
        all_data.append({'Header': header_tuple, 'Value': cleaned_val})
    # Subtable 2
    years = []
    year_row = df_data.iloc[year_header_row]
    for c_idx in range(1, len(df_data.columns)):
        val = str(year_row.iloc[c_idx]).strip()
        if year_pattern.fullmatch(val):
            years.append((c_idx, val))
    for r_idx in range(year_header_row + 1, len(df_data)):
        category = str(df_data.iat[r_idx, 0]).replace('\n', ' ').strip()
        if not category or category.lower() in ['nan', '-', '']:
            continue
        for c_idx, year_label in years:
            if c_idx >= len(df_data.columns):
                continue
            value = str(df_data.iat[r_idx, c_idx]).strip()
            if value == '0':
                cleaned_val = 0
            elif value.lower() in ['', '-', 'nan']:
                cleaned_val = None
            else:
                cleaned_val = value
            header_tuple = (table_heading_clean, year_label, category)
            all_data.append({'Header': header_tuple, 'Value': cleaned_val})
    return True

# --- Convert merged tables into records using all of the mapping heuristics ---
def extract_pdf_to_records(pdf_path: str) -> List[Dict]:
    merged_tables = extract_and_group_tables(pdf_path)
    if not merged_tables:
        return []
    records: List[Dict] = []
    year_pattern = re.compile(r'(20\d{2}-\d{2}|20\d{2}-20\d{2})')

    for table_heading, df in merged_tables.items():
        table_content = df.astype(str).values.flatten()
        table_text = ' '.join(table_content).lower()
        is_expenditure_table = (
            "annual capital expenditure" in table_text or
            "annual operational expenditure" in table_text or
            "library" in table_text or
            "equipment" in table_text or
            "engineering workshop" in table_text or
            "salary" in table_text or
            "seminars" in table_text
        )
        if not is_expenditure_table:
            continue

        try:
            if df.shape[1] < 2 or df.shape[0] < 2:
                continue
            header_row_index, data_start_index, new_header = find_header_and_data_start(df)
            lowered_headers = [str(h).lower() for h in new_header]
            contains_qualification = any("qualification" in h for h in lowered_headers)
            contains_designation = any("designation" in h for h in lowered_headers)
            contains_gender = any(h == "gender" for h in lowered_headers)

            if contains_qualification and contains_designation:
                if data_start_index >= len(df):
                    continue
                df_data = df.iloc[data_start_index:].copy()
                if len(new_header) != len(df_data.columns):
                    new_header = df_data.columns.tolist()
                seen_headers, clean_headers = {}, []
                for h in new_header:
                    if h in seen_headers:
                        seen_headers[h] += 1
                        clean_headers.append(f"{h}_{seen_headers[h]}")
                    else:
                        seen_headers[h] = 0
                        clean_headers.append(h)
                df_data.columns = clean_headers
                def find_col(target):
                    for c in df_data.columns:
                        if str(c).strip().lower() == target:
                            return c
                    return None
                gender_col = find_col('gender') if contains_gender else None
                if gender_col:
                    gender_series = df_data[gender_col].astype(str).str.replace('\n', ' ', regex=False).str.strip()
                    gender_series = gender_series[gender_series.str.len() > 0]
                    for gender, count in gender_series.value_counts(dropna=True).items():
                        header_tuple = (table_heading, f"Number of {gender}", '')
                        records.append({'Header': header_tuple, 'Value': int(count)})
                continue

            if contains_qualification:
                continue
            if data_start_index >= len(df):
                continue
            if df.shape[1] == 2 and data_start_index > 0:
                data_start_index = 0
            df_data = df.iloc[data_start_index:].copy()

            if 'ph.d (student pursuing doctoral program' in str(new_header[0]).lower() and df_data.shape[1] >= 3:
                if process_combined_phd_table(df_data, table_heading, records):
                    continue

            if df_data.empty:
                continue
            if len(new_header) != len(df_data.columns):
                new_header = df_data.columns.tolist()
            seen_headers, clean_headers = {}, []
            for h in new_header:
                if h in seen_headers:
                    seen_headers[h] += 1
                    clean_headers.append(f"{h}_{seen_headers[h]}")
                else:
                    seen_headers[h] = 0
                    clean_headers.append(h)
            df_data.columns = clean_headers

            if len(df_data.columns) == 2:
                col_key, col_val = df_data.columns.tolist()
                for _, r in df_data.iterrows():
                    key_label = str(r[col_key]).replace('\n', ' ').strip()
                    if not key_label or key_label.lower() in ['-', 'nan']:
                        continue
                    val = r[col_val]
                    val_str = str(val).strip()
                    if val_str == '0':
                        cleaned_val = 0
                    elif pd.isna(val) or val_str.lower() in ['', '-', 'nan']:
                        cleaned_val = None
                    else:
                        cleaned_val = val_str
                    header_tuple = (table_heading, key_label, '')
                    records.append({'Header': header_tuple, 'Value': cleaned_val})
                continue

            if clean_headers.count('Academic Year') > 1 or any(h.startswith('Academic Year_') for h in clean_headers):
                recs = process_pivot_table(df_data, clean_headers, table_heading)
                records.extend(recs)
                continue

            year_columns = [h for h in clean_headers if year_pattern.search(str(h))]
            id_col = clean_headers[0]

            if year_columns:
                for year_col in year_columns:
                    year_match = year_pattern.search(str(year_col))
                    if year_match:
                        year_value = year_match.group(0)
                        for _, row in df_data.iterrows():
                            category = str(row[id_col]).replace('\n', ' ').strip()
                            if not category or category.lower() in ['-', 'nan', 'none']:
                                continue
                            raw_val = row.get(year_col)
                            val_str = str(raw_val).strip()
                            if val_str == '0':
                                cleaned_val = 0
                            elif pd.isna(raw_val) or val_str.lower() in ['', '-', 'nan', 'none']:
                                cleaned_val = None
                            else:
                                cleaned_val = raw_val
                            metric = str(year_col).replace(year_value, '').strip()
                            if not metric:
                                metric = f"Value for {year_value}"
                            header_tuple = (table_heading, f"{metric} ({year_value})", category)
                            records.append({'Header': header_tuple, 'Value': cleaned_val})
            else:
                melted = df_data.melt(id_vars=[id_col], var_name='ColumnHeader', value_name='Value')
                for _, row in melted.iterrows():
                    raw_val = row['Value']
                    val_str = str(raw_val).strip()
                    if val_str == '0':
                        cleaned_val = 0
                    elif pd.isna(raw_val) or val_str.lower() in ['', '-', 'nan']:
                        cleaned_val = None
                    else:
                        cleaned_val = raw_val
                    row_cat = str(row[id_col]).replace('\n', ' ').strip()
                    col_cat = str(row['ColumnHeader']).replace('\n', ' ').strip()
                    year_in_row = year_pattern.search(row_cat)
                    year_in_col = year_pattern.search(col_cat)
                    if year_in_row:
                        year_value = year_in_row.group(0)
                        row_cat = row_cat.replace(year_value, '').strip()
                        header_tuple = (table_heading, f"{col_cat} ({year_value})", row_cat)
                    elif year_in_col:
                        year_value = year_in_col.group(0)
                        col_cat = col_cat.replace(year_value, '').strip()
                        header_tuple = (table_heading, f"{col_cat} ({year_value})", row_cat)
                    else:
                        header_tuple = (table_heading, col_cat, row_cat)
                    records.append({'Header': header_tuple, 'Value': cleaned_val})
        except Exception:
            # continue on any table-level error; mimic original behavior
            continue

    return records
def map_records_to_dataframe_full(pdf_records: List[Dict], pdf_filename: str, original_filename: str) -> pd.DataFrame:
    """
    Fully identical to append_to_csv(), but instead of writing CSV,
    returns a 1-row DataFrame with the 25 standard NIRF financial columns.
    """
    data_map = {}
    year_pattern = re.compile(r'(20\d{2}-\d{2}|20\d{2}-20\d{2})')

    for rec in pdf_records:
        header_tuple = rec['Header']
        value = rec['Value']

        if value is None or str(value).strip() == "":
            continue

        table_heading = str(header_tuple[0]).strip().lower()
        metric = str(header_tuple[1]).strip().lower()
        category = str(header_tuple[2]).strip().lower()

        full_content = f"{table_heading} {metric} {category}".lower()

        # ---- complex_capital_asset identical to original ----
        capital_asset_pattern = re.compile(
            r'other expenditure on creation of capital assets.*\(for setting up|excluding.*land',
            re.IGNORECASE
        )
        complex_capital_asset = any(
            isinstance(p, str) and capital_asset_pattern.search(p.lower())
            for p in header_tuple
        )

        # ---- library detection identical ----
        library_pattern = re.compile(r'library\s*\(\s*books', re.IGNORECASE)
        is_library_record = any(
            isinstance(p, str) and library_pattern.search(p.lower())
            for p in header_tuple
        )
        if not is_library_record:
            is_library_record = any(
                isinstance(p, str) and p.lower().strip().startswith("library")
                for p in header_tuple
            )

        is_studio_record = any(
            isinstance(p, str) and p.lower().strip() == "studios"
            for p in header_tuple
        )

        # ---- Skip nonsense ----
        if "median salary" in full_content or "placed graduates" in full_content:
            continue

        # ---- Category clustering identical ----
        is_capital_data = False
        is_operational_data = False

        if (
            "annual capital expenditure" in full_content or
            "library expenditure" in full_content or
            "expenditure on library" in full_content or
            is_library_record or
            is_studio_record or
            "studios" in full_content or
            ("equipment" in full_content and "laborator" in full_content) or
            "laboratory equipment" in full_content or
            ("engineering workshop" in full_content and "seminar" not in full_content) or
            ("studio" in full_content and not any(ne in full_content for ne in ["student", "graduating", "placed"])) or
            ("capital asset" in full_content and "other" not in full_content)
        ):
            is_capital_data = True

        is_salary_record = "salaries (teaching and non teaching staff)" in full_content
        is_seminar_record = "seminars/conferences/workshops" in full_content

        if is_salary_record or is_seminar_record:
            is_operational_data = True
        elif (
            "annual operational expenditure" in full_content or
            ("salary expenditure" in full_content or ("salary" in full_content and "expenditure" in full_content) or
             ("salaries" in full_content and "teaching" in full_content and "non teaching" in full_content)) and
            not any(ex in full_content for ex in ["median", "placed", "graduate"]) or
            ("maintenance" in full_content and "infrastructure" in full_content) or
            ("seminar/conference/workshop" in full_content or
             (("seminar" in full_content or "conference" in full_content or "workshop" in full_content) and
              "expenditure" in full_content))
        ):
            is_operational_data = True

        if "other expenditure" in full_content and ("capital" in full_content or "asset" in full_content):
            is_capital_data = True
            is_operational_data = False

        if not (is_capital_data or is_operational_data):
            continue

        # ---- year detection identical ----
        year_match = year_pattern.search(full_content)
        year_found = year_match.group(0) if year_match else None

        if not year_found:
            for year in ["2023-24", "2022-23", "2021-22"]:
                for related_rec in pdf_records:
                    related_header = related_rec["Header"]
                    if len(related_header) >= 2:
                        related_content = " ".join(
                            [str(p).strip().lower() for p in related_header]
                        )
                        if year in related_content and any(
                            kw in full_content for kw in [metric, category, table_heading]
                        ):
                            year_found = year
                            break
                if year_found:
                    break

        if not year_found:
            current_year = datetime.now().year
            if "current" in full_content or str(current_year) in full_content:
                year_found = "2023-24"
            elif "previous" in full_content or str(current_year - 1) in full_content:
                year_found = "2022-23"
            elif "before" in full_content or str(current_year - 2) in full_content:
                year_found = "2021-22"

        if not year_found:
            if "3" in metric:
                year_found = "2021-22"
            elif "2" in metric:
                year_found = "2022-23"
            elif "1" in metric:
                year_found = "2023-24"

        if not year_found:
            continue

        # ---- identical mapping section ----
        mapped = False

        # CAPITAL
        if is_capital_data:
            if is_library_record:
                data_map[f"Library ({year_found})"] = value; mapped = True

            elif "equipment" in full_content and "laborator" in full_content:
                data_map[f"New Equipment for Laboratories ({year_found})"] = value; mapped = True

            elif "engineering workshop" in full_content:
                data_map[f"Engineering Workshops ({year_found})"] = value; mapped = True

            elif is_studio_record or "studio" in full_content:
                data_map[f"Studios ({year_found})"] = value; mapped = True

            elif (
                "other expenditure" in full_content and "capital" in full_content
            ) or "creation of capital asset" in full_content:
                data_map[f"Other expenditure on creation of Capital Assets ({year_found})"] = value; mapped = True

        # OPERATIONAL
        elif is_operational_data:
            if is_salary_record or (
                "salary" in full_content and "expenditure" in full_content
            ):
                data_map[f"Salaries ({year_found})"] = value; mapped = True

            elif "maintenance" in full_content and "infrastructure" in full_content:
                data_map[f"Maintenance of Academic Infrastructure or consumables and other running expenditures ({year_found})"] = value; mapped = True

            elif is_seminar_record or (
                ("seminar" in full_content or "workshop" in full_content or "conference" in full_content)
                and "expenditure" in full_content
            ):
                data_map[f"Seminars/Conferences/Workshops ({year_found})"] = value; mapped = True

        # fallback identical
        if not mapped and (
            complex_capital_asset or 
            "other expenditure on creation of capital assets" in full_content
        ):
            data_map[f"Other expenditure on creation of Capital Assets ({year_found})"] = value


    # -------- final row identical --------
    college_name = original_filename.replace(".pdf", "").replace("_", " ").title()
    row = [college_name] + [data_map.get(col, "") for col in CSV_COLUMNS[1:]]
    df = pd.DataFrame([row], columns=CSV_COLUMNS)
    return df


# --- The only exported convenience function the backend will use ---
def extract_financial_data(pdf_path: str,college_type: str,original_filename: str) -> pd.DataFrame:
    """
    Primary backend entrypoint you asked for (Option A).
    - Runs extract_pdf_to_records(pdf_path) using the full extraction logic above.
    - Maps values into the fixed 25 CSV_COLUMNS.
    - Returns a pandas DataFrame with 1 row (college name + mapped fields).
    - If no records found, returns empty DataFrame.
    """
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF not found: {pdf_path}")
    print("\nStarting table extraction and grouping (single PDF mode)...")
    merged_tables = extract_and_group_tables(pdf_path)
    if merged_tables:
        recs = extract_pdf_to_records(pdf_path)
        print("\nmapping start")
        df=map_records_to_dataframe_full(recs, os.path.basename(pdf_path),original_filename=original_filename)
        return df
    else:
        print("No tables were extracted from the PDF.")
# If you want to test quickly from Python REPL:
# from app.services.pdf_extractor_backend import extract_financial_data
# df = extract_financial_data("path/to/your.pdf")
# print(df.shape); print(df.head(1))
