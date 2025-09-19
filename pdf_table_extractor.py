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


def extract_tables_from_pdf(pdf_path, pages='all'):
    """
    Extract tables from a PDF file using Camelot.
    
    Parameters:
    pdf_path (str): Path to the PDF file
    pages (str or list): Pages to extract from ('all' or list of page numbers)
    flavor (str): Extraction method ('lattice' or 'stream')
    
    Returns:
    dict: Dictionary mapping headings to merged pandas DataFrames
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
        # Step 1: Extract headings/non-table text using fitz
        doc = fitz.open(pdf_path)
        headings = []
        for page_num in range(len(doc)):
            page = doc[page_num]
            blocks = page.get_text("blocks")
            for b in blocks:
                x0, y0, x1, y1, text, block_no, block_type = b[:7]
                # Heuristic: consider text blocks with larger font size or bold as headings
                # But fitz does not provide font info in blocks, so use position and length
                if text.strip() and len(text.strip()) < 100 and y1 < page.rect.height * 0.5:
                    headings.append({
                        "text": text.strip(),
                        "page": page_num + 1,
                        "y1": y1
                    })
        # Step 2: Associate tables with nearest heading above
        table_info = []
        for i, table in enumerate(tables):
            df = table.df
            if len(df) == 0:
                continue
            page = int(table.page)
            # Get table bbox (top y)
            bbox = table._bbox
            table_top = bbox[1] if bbox else None
            # Find nearest heading above
            heading_text = None
            candidates = [h for h in headings if h["page"] == page and (table_top is None or h["y1"] < table_top)]
            if candidates:
                # Pick the heading with largest y1 below table_top
                heading_text = max(candidates, key=lambda h: h["y1"])["text"]
            else:
                # If no heading above, use previous page's last heading
                prev_candidates = [h for h in headings if h["page"] < page]
                if prev_candidates:
                    heading_text = prev_candidates[-1]["text"]
            if not heading_text:
                heading_text = f"No Heading (Table {i+1})"
            table_info.append({
                "heading": heading_text,
                "df": df
            })
        # Step 3: Merge tables under the same heading
        merged_tables = {}
        for info in table_info:
            heading = info["heading"]
            df = info["df"]
            if heading in merged_tables:
                # Merge with previous table under same heading
                merged_tables[heading] = pd.concat([merged_tables[heading], df], ignore_index=True)
            else:
                merged_tables[heading] = df
        # Print merged tables
        for heading, df in merged_tables.items():
            print(f"\nHeading: {heading}")
            print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
            print("=" * 50)
            print(df.to_string(index=False))
            print("=" * 50)
        return merged_tables
    except Exception as e:
        print(f"Error extracting tables: {str(e)}")
        return {}


def save_tables_to_csv(merged_tables, output_dir="output", pdf_name="extracted_tables"):
    """
    Save merged tables to CSV files.
    
    Parameters:
    merged_tables (dict): Dictionary mapping headings to DataFrames
    output_dir (str): Directory to save CSV files
    pdf_name (str): Base name for output files
    """
    if not merged_tables:
        print("No tables to save.")
        return
    Path(output_dir).mkdir(exist_ok=True)
    import re
    for i, (heading, df) in enumerate(merged_tables.items()):
        # Remove newlines and replace non-alphanumeric (except space/underscore) with underscore
        safe_heading = heading.replace("/", "_").replace("\\", "_").replace(":", "_").replace("|", "_")
        safe_heading = safe_heading.replace("\n", "_").replace("\r", "_")
        safe_heading = re.sub(r'[^\w\s]', '_', safe_heading)  # keep alphanumeric, underscore, space
        safe_heading = re.sub(r'\s+', '_', safe_heading)  # replace spaces with underscore
        filename = f"{output_dir}/{pdf_name}_{safe_heading}_table_{i+1}.csv"
        df.to_csv(filename, index=False)
        print(f"Saved table under heading '{heading}' to: {filename}")


def main():
    """
    Main function to demonstrate PDF table extraction.
    """
    print("PDF Table Extractor using Camelot")
    print("=" * 40)
    
    # Example usage - you can modify this path
    pdf_path = "iit_delhi_data.pdf"
    
    if not pdf_path:
        print("No PDF path provided. Using example...")
        print("Please place a PDF file in the current directory and run the script again.")
        return
    
    # Try different extraction methods
    print("\nExtracting tables using lattice method...")
    merged_tables = extract_tables_from_pdf(pdf_path)
    if not merged_tables:
        print("No tables could be extracted with lattice method.")
        return
    save_tables_to_csv(merged_tables, pdf_name=Path(pdf_path).stem)


if __name__ == "__main__":
    main()
