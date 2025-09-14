#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import re

RAW = Path("raw_data")
OUT = Path("silver/revenue_details.parquet")
OUT.parent.mkdir(exist_ok=True, parents=True)

def parse_amount_brl(x):
    if pd.isna(x): return None
    s = str(x)
    s = re.sub(r"[^0-9,.\-]", "", s)
    s = s.replace(".", "").replace(",", ".")
    try: return round(float(s), 2)
    except: return None

def main():
    path = RAW / "revenue.csv"
    if not path.exists():
        print(f"{path} not found. Run the extraction script first.")
        return

    print(f"Processing {path.name}")
    df = pd.read_csv(path, sep=';')
    
    df = df.iloc[:, [5, 8]]
    df.columns = ["week", "net_brl"]
    
    df = df[df["week"].str.contains("Total", na=False) == False]
    df = df.dropna(subset=["week"])
    
    df['date_str'] = df['week'].str.split(' - ').str[0]
    df['date'] = pd.to_datetime(df['date_str'], errors='coerce')

    # For dates that failed parsing, try to extract the year and create a date
    mask = df['date'].isna()
    df.loc[mask, 'date'] = pd.to_datetime(df.loc[mask, 'week'].str.extract(r'(\d{4})')[0], format='%Y', errors='coerce')

    df["month"] = pd.to_datetime(df["date"].dt.to_period("M").dt.start_time)
    
    df["amount_brl"] = df["net_brl"].apply(parse_amount_brl)
    df["source_file"] = "03 TradeDesk (BRAZIL)  - PNL & Volume Report (2).xlsx"
    df["desk"] = "TradeDesk"
    df["product"] = "OTC"
    df["source"] = "TradeDesk"
    df["notes"] = ""

    df = df[["date", "month", "amount_brl", "source", "desk", "product", "notes", "source_file"]]
    
    df.to_parquet(OUT, index=False)
    print(f"Wrote {len(df)} rows to {OUT}")

if __name__ == "__main__":
    main()
