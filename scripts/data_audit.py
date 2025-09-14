#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import glob
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def audit_csv(path):
    logging.info(f"\n--- Auditing CSV: {path.name} ---")
    try:
        df = pd.read_csv(path, sep=';')
        logging.info(f"Columns: {df.columns.tolist()}")
        logging.info("First 5 rows:")
        logging.info(df.head().to_string())
        logging.info(f"Row count: {len(df)}")
        logging.info("Data types:")
        logging.info(df.dtypes)
    except Exception as e:
        logging.error(f"Could not read CSV: {e}")

def audit_excel(path):
    logging.info(f"\n--- Auditing Excel: {path.name} ---")
    try:
        xls = pd.ExcelFile(path)
        for sheet_name in xls.sheet_names:
            logging.info(f"\n--- Sheet: {sheet_name} ---")
            df = pd.read_excel(xls, sheet_name=sheet_name)
            logging.info(f"Columns: {df.columns.tolist()}")
            logging.info("First 5 rows:")
            logging.info(df.head().to_string())
            logging.info(f"Row count: {len(df)}")
            logging.info("Data types:")
            logging.info(df.dtypes)
    except Exception as e:
        logging.error(f"Could not read Excel file: {e}")

def main():
    logging.info("--- Starting Data Audit ---")

    # Audit Expenses
    expense_workbook_path = next(Path("raw_data").glob("01 Expenses Management*.xlsx"), None)
    if expense_workbook_path:
        audit_excel(expense_workbook_path)
    else:
        logging.warning("\nExpense workbook not found.")

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
        logging.warning("\nRevenue workbook not found.")

    logging.info("\n--- Data Audit Complete ---")

if __name__ == "__main__":
    main()
