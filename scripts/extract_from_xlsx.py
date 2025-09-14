#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import glob

def main():
    # Extract Expenses
    expense_workbook_path = next(Path("raw_data").glob("01 Expenses Management*.xlsx"), None)
    if expense_workbook_path:
        print(f"Extracting from {expense_workbook_path.name}")
        xls = pd.ExcelFile(expense_workbook_path)
        if "History" in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name="History")
            df.to_csv("raw_data/expenses.csv", sep=';', index=False)
            print("Successfully extracted 'History' sheet to 'raw_data/expenses.csv'")
        if "Credit Card History" in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name="Credit Card History")
            df.to_csv("raw_data/credit_card_history.csv", sep=';', index=False)
            print("Successfully extracted 'Credit Card History' sheet to 'raw_data/credit_card_history.csv'")
    else:
        print("Expense workbook not found.")

    # Extract Revenue
    revenue_workbook_path = next(Path("raw_data").glob("03 TradeDesk (BRAZIL)  - PNL & Volume Report (2).xlsx"), None)
    if revenue_workbook_path:
        print(f"Extracting from {revenue_workbook_path.name}")
        xls = pd.ExcelFile(revenue_workbook_path)
        if "Weekly Summary" in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name="Weekly Summary", skiprows=7)
            df.to_csv("raw_data/revenue.csv", sep=';', index=False)
            print("Successfully extracted 'Weekly Summary' sheet to 'raw_data/revenue.csv'")
    else:
        print("Revenue workbook not found.")

if __name__ == "__main__":
    main()
