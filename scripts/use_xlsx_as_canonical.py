#!/usr/bin/env python3
import pandas as pd
import argparse
from pathlib import Path
import glob

def main():
    parser = argparse.ArgumentParser(description="Extract sheets from an Excel workbook and save them as CSVs.")
    parser.add_argument("--path", help="Path to the Excel workbook.")
    args = parser.parse_args()

    if args.path:
        workbook_path = Path(args.path)
    else:
        files = glob.glob("raw_data/01 Expenses Management*.xlsx")
        if not files:
            print("No '01 Expenses Management*.xlsx' file found in raw_data/")
            return
        workbook_path = max(files, key=lambda f: Path(f).stat().st_mtime)

    print(f"Using workbook: {workbook_path}")

    sheets_to_export = {
        "Analysis": "Analysis-Analysis 2.csv",
        "Visuals": "Analysis-Analysis 4.csv",
    }

    for sheet_name, csv_filename in sheets_to_export.items():
        print(f"Exporting sheet '{sheet_name}' to '{csv_filename}'...")
        try:
            df = pd.read_excel(workbook_path, sheet_name=sheet_name, skiprows=1)
            output_path = Path("raw_data") / csv_filename
            df.to_csv(output_path, sep=';', index=False)
            print(f"Successfully exported '{sheet_name}' to '{output_path}'")
        except Exception as e:
            print(f"Could not export sheet '{sheet_name}': {e}")

if __name__ == "__main__":
    main()
