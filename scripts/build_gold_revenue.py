#!/usr/bin/env python3
import pandas as pd
import duckdb
from pathlib import Path
import argparse
import os
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SILVER = Path("silver/revenue_details.parquet")
GOLD = Path("gold")
GOLD.mkdir(exist_ok=True, parents=True)

def get_year():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Reporting year")
    args = parser.parse_args()
    if args.year:
        return args.year
    if os.getenv("REPORTING_YEAR"):
        return int(os.getenv("REPORTING_YEAR"))
    return datetime.now().year

def main(year):
    if not SILVER.exists():
        logging.warning(f"{SILVER} not found. Run the silver revenue script first.")
        return

    df = pd.read_parquet(SILVER)
    df["tx_date"] = pd.to_datetime(df["date"]).dt.date
    df = df.rename(columns={"amount_brl": "net_brl"})

    # Gold transactions
    tx = df[["tx_date", "month", "net_brl", "desk", "source", "source_file"]]
    tx.to_parquet(GOLD / "revenue_tx.parquet", index=False)
    logging.info(f"Wrote {len(tx)} rows to {GOLD / 'revenue_tx.parquet'}")

    # Gold per month
    bars = (tx.groupby(["month"], as_index=False)["net_brl"].sum()
              .sort_values(["month"]))
    bars.to_parquet(GOLD / "revenue_per_month.parquet", index=False)
    bars.to_csv(GOLD / "revenue_per_month.csv", index=False)
    logging.info(f"Wrote {len(bars)} rows to {GOLD / 'revenue_per_month.parquet'}")

    # Gold cumulative year
    my = tx[tx["month"].dt.year == year]
    if not my.empty:
        p = (my.groupby(["month"], as_index=False)["net_brl"].sum()
                     .sort_values(["month"]))
        p["cum_total"] = p["net_brl"].cumsum()
        p.to_parquet(GOLD / f"revenue_cumulative_{year}.parquet", index=False)
        p.to_csv(GOLD / f"revenue_cumulative_{year}.csv", index=False)
        logging.info(f"Wrote {len(p)} rows to {GOLD / f'revenue_cumulative_{year}.parquet'}")

if __name__ == "__main__":
    year = get_year()
    main(year)
