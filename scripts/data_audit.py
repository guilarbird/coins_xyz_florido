#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import glob

def audit_csv(path):
    print(f"\n--- Auditing CSV: {path.name} ---")
    try:
        df = pd.read_csv(path, sep=';')
        print("Columns:", df.columns.tolist())
        print("First 5 rows:")
        print(df.head().to_string())
        print("Row count:", len(df))
        print("Data types:")
        print(df.dtypes)
    except Exception as e:
        print(f"Could not read CSV: {e}")

def audit_excel(path):
    print(f"\n--- Auditing Excel: {path.name} ---")
    try:
        xls = pd.ExcelFile(path)
        for sheet_name in xls.sheet_names:
            print(f"\n--- Sheet: {sheet_name} ---")
            df = pd.read_excel(xls, sheet_name=sheet_name)
            print("Columns:", df.columns.tolist())
            print("First 5 rows:")
            print(df.head().to_string())
            print("Row count:", len(df))
            print("Data types:")
            print(df.dtypes)
    except Exception as e:
        print(f"Could not read Excel file: {e}")

def main():
    print("--- Starting Data Audit ---")

    # Audit Expenses
    expense_workbook_path = next(Path("raw_data").glob("01 Expenses Management*.xlsx"), None)
    if expense_workbook_path:
        audit_excel(expense_workbook_path)
    else:
        print("\nExpense workbook not found.")

    expense_csv_paths = [
        "raw_data/Analysis-Analysis 2.csv",
        "raw_data/Analysis-Analysis 4.csv",
        "raw_data/History-Table 1.csv",
        "raw_data/Credit Card History-Table 1.csv"
    ]
    for path in expense_csv_paths:
        audit_csv(Path(path))

    # Audit Revenue
    revenue_workbook_path = next(Path("raw_data").glob("03 TradeDesk (BRAZIL)  - PNL & Volume Report (2).xlsx"), None)
    if revenue_workbook_path:
        audit_excel(revenue_workbook_path)
    else:
        print("\nRevenue workbook not found.")

    print("\n--- Data Audit Complete ---")

if __name__ == "__main__":
    main()
