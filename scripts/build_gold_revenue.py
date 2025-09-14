#!/usr/bin/env python3
import pandas as pd
import duckdb
from pathlib import Path

SILVER = Path("silver/revenue_details.parquet")
GOLD = Path("gold")
GOLD.mkdir(exist_ok=True, parents=True)

def main():
    if not SILVER.exists():
        print(f"{SILVER} not found. Run the silver revenue script first.")
        return

    df = pd.read_parquet(SILVER)
    df["tx_date"] = pd.to_datetime(df["date"]).dt.date
    df = df.rename(columns={"amount_brl": "net_brl"})

    # Gold transactions
    tx = df[["tx_date", "month", "net_brl", "desk", "source", "source_file"]]
    tx.to_parquet(GOLD / "revenue_tx.parquet", index=False)
    print(f"Wrote {len(tx)} rows to {GOLD / 'revenue_tx.parquet'}")

    # Gold per month
    bars = (tx.groupby(["month"], as_index=False)["net_brl"].sum()
              .sort_values(["month"]))
    bars.to_parquet(GOLD / "revenue_per_month.parquet", index=False)
    bars.to_csv(GOLD / "revenue_per_month.csv", index=False)
    print(f"Wrote {len(bars)} rows to {GOLD / 'revenue_per_month.parquet'}")

    # Gold cumulative 2025
    m2025 = tx[tx["month"].dt.year == 2025]
    if not m2025.empty:
        p = (m2025.groupby(["month"], as_index=False)["net_brl"].sum()
                     .sort_values(["month"]))
        p["cum_total"] = p["net_brl"].cumsum()
        p.to_parquet(GOLD / "revenue_cumulative_2025.parquet", index=False)
        p.to_csv(GOLD / "revenue_cumulative_2025.csv", index=False)
        print(f"Wrote {len(p)} rows to {GOLD / 'revenue_cumulative_2025.parquet'}")

if __name__ == "__main__":
    main()
