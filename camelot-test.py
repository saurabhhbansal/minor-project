import camelot
import pandas as pd

# Specify the path to the PDF file
pdf_path = "example.pdf"  # Replace with your PDF file path

try:
    # Extract tables from the PDF
    tables = camelot.read_pdf(pdf_path, pages='all')

    # Check if tables were found
    if tables.n > 0:
        for i, table in enumerate(tables):
            print(f"Table {i + 1}:")
            # Convert table to pandas DataFrame
            df = table.df
            print(df)
            print("\n")
    else:
        print("No tables found in the PDF.")

except Exception as e:
    print(f"An error occurred: {e}")