#!/usr/bin/env python3
import duckdb
import pandas as pd
from pathlib import Path
import re
import argparse
import os
from datetime import datetime

def get_year():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Reporting year")
    args = parser.parse_args()
    if args.year:
        return args.year
    if os.getenv("REPORTING_YEAR"):
        return int(os.getenv("REPORTING_YEAR"))
    return datetime.now().year

def parse_amount_brl(x):
    if pd.isna(x): return None
    s = str(x)
    s = re.sub(r"[^0-9,.\-]", "", s)
    s = s.replace(".", "").replace(",", ".")
    try: return round(float(s), 2)
    except: return None

def main(year):
    with duckdb.connect("warehouse/coins_xyz.duckdb", read_only=True) as con:
        print("--- Verifying Expenses ---")
        try:
            expenses_df = con.execute("SELECT * FROM gold__expenses_per_month_status").df()
            print("gold__expenses_per_month_status:")
            print(expenses_df.to_string(index=False))

            expenses_cum_df = con.execute(f"SELECT * FROM gold__expenses_cumulative_{year}").df()
            print(f"\ngold__expenses_cumulative_{year}:")
            print(expenses_cum_df.to_string(index=False))

            # Load expected values from the CSV file
            expected_df = pd.read_csv("raw_data/Analysis-Analysis 2.csv", sep=';', skiprows=2)
            expected_df = expected_df.rename(columns={'Status': 'date', 'Completed': 'amount'})
            expected_df = expected_df.dropna(subset=["date"])
            expected_df["month"] = pd.to_datetime(expected_df["date"], format="%d-%m-%Y", errors='coerce').dt.to_period('M').dt.start_time

            expected_completed = expected_df.groupby("month")["amount"].sum().reset_index()
            expected_projected = pd.DataFrame(columns=['month', 'amount'])
            if 'Projected' in expected_df.columns:
                projected_df = pd.read_csv("raw_data/Analysis-Analysis 2.csv", sep=';', skiprows=2)
                projected_df = projected_df.rename(columns={"Date": "date", "Projected": "amount"})
                projected_df = projected_df.dropna(subset=["date"])
                projected_df["month"] = pd.to_datetime(projected_df["date"], format="%d-%m-%Y", errors='coerce').dt.to_period('M').dt.start_time
                expected_projected = projected_df.groupby("month")["amount"].sum().reset_index()

            expenses_df['month'] = pd.to_datetime(expenses_df['month'])
            
            completed_df = expenses_df[expenses_df['status'] == 'Completed']
            projected_df = expenses_df[expenses_df['status'] == 'Projected']

            expected_completed['amount'] = expected_completed['amount'].apply(parse_amount_brl)
            expected_projected['amount'] = expected_projected['amount'].apply(parse_amount_brl)

            merged_completed = pd.merge(completed_df, expected_completed, on="month", how="outer", suffixes=("_actual", "_expected")).fillna(0)
            merged_projected = pd.merge(projected_df, expected_projected, on="month", how="outer", suffixes=("_actual", "_expected")).fillna(0)

            merged_completed['amount_expected'] = merged_completed['amount_expected'].apply(parse_amount_brl)
            merged_projected['amount_expected'] = merged_projected['amount_expected'].apply(parse_amount_brl)

            completed_diff = (merged_completed["amount_actual"] - merged_completed["amount_expected"]).abs().max()
            projected_diff = (merged_projected["amount_actual"] - merged_projected["amount_expected"]).abs().max()

            print(f"\nMax difference for completed expenses: {completed_diff:.2f}")
            print(f"Max difference for projected expenses: {projected_diff:.2f}")

            if completed_diff > 0.01 or projected_diff > 0.01:
                print("\nVerification FAILED: Expenses do not match the workbook.")
                exit(1)
            else:
                print("\nVerification PASSED: Expenses match the workbook.")

        except Exception as e:
            print(f"Error verifying expenses: {e}")
            exit(1)


        print("\n--- Verifying Revenue ---")
        try:
            revenue_df = con.execute("SELECT * FROM gold__revenue_per_month").df()
            print("gold__revenue_per_month:")
            print(revenue_df.to_string(index=False))

            revenue_cum_df = con.execute(f"SELECT * FROM gold__revenue_cumulative_{year}").df()
            print(f"\ngold__revenue_cumulative_{year}:")
            print(revenue_cum_df.to_string(index=False))
            
            if revenue_df.empty:
                print("\nRevenue data is empty, but this is expected if the TradeDesk file is not present.")
            else:
                print("\nRevenue data is present.")

        except Exception as e:
            print(f"Error verifying revenue: {e}")
            exit(1)

if __name__ == "__main__":
    year = get_year()
    main(year)
