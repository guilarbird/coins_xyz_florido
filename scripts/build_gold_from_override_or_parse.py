#!/usr/bin/env python3
import pandas as pd, duckdb, os, re
from pathlib import Path
from datetime import datetime
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

OUT = Path("gold"); OUT.mkdir(exist_ok=True)
DB  = "warehouse/coins_xyz.duckdb"

def get_year():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Reporting year")
    args = parser.parse_args()
    if args.year:
        return args.year
    if os.getenv("REPORTING_YEAR"):
        return int(os.getenv("REPORTING_YEAR"))
    return datetime.now().year

def write_gold(df, year):
    # long canon
    canon = (
        pd.concat([
            df.assign(status="Completed", amount=df["completed"])[["month","status","amount"]],
            df.assign(status="Projected", amount=df["projected"])[["month","status","amount"]],
        ])
        .query("amount != 0")
        .assign(tx_date=lambda d: d["month"])
        [["tx_date","month","amount","status"]]
        .sort_values(["month","status"])
        .reset_index(drop=True)
    )
    canon.to_parquet(OUT/"expenses_canon.parquet", index=False)

    # per-month/status
    per = (canon.groupby(["month","status"], as_index=False)["amount"]
                 .sum().rename(columns={"amount":"total"}))
    per.to_parquet(OUT/"expenses_per_month_status.parquet", index=False)
    per.to_csv(OUT/"expenses_per_month_status.csv", index=False)

    # cumulative year
    y = df[df["month"].dt.year==year].copy()
    y["cum_completed"] = y["completed"].cumsum()
    y["cum_total"]     = (y["completed"] + y["projected"]).cumsum()
    y[["month","cum_completed","cum_total"]].to_parquet(OUT/f"expenses_cumulative_{year}.parquet", index=False)
    y[["month","cum_completed","cum_total"]].to_csv(OUT/f"expenses_cumulative_{year}.csv", index=False)

def parse_visuals_fallback(year):
    # very light fallback: try to read the “status” pivot from analysis_table_1
    try:
        with duckdb.connect(DB) as con:
            df = con.execute("SELECT * FROM silver__analysis_table_1__analysis_table_1").df()
    except Exception:
        return None
    lines = []
    for s in df.iloc[:,0].astype(str):
        toks = [t.strip() for t in s.split(";")]
        if any(re.match(r"\d{4}-(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)\.?$", t) for t in toks):
            lines.append(toks)
    # crude extraction (best-effort)
    month_map = {"jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,"jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12}
    rows=[]
    for toks in lines:
        try:
            m = re.search(fr"{year}-(\w+)", " ".join(toks)).group(1).strip(".").lower()
            month = pd.Timestamp(year=year, month=month_map[m], day=1)
            # pick the two biggest numbers on the line as completed/projected candidates
            nums = []
            for t in toks:
                tt = re.sub(r"[^0-9,.-]","",t).replace(".","").replace(",",".")
                try: 
                    if tt and tt not in ("-","."): nums.append(float(tt))
                except: pass
            nums = sorted(nums, reverse=True)
            comp = nums[0] if nums else 0.0
            proj = nums[1] if len(nums)>1 else 0.0
            rows.append({"month":month,"completed":comp,"projected":proj})
        except Exception:
            pass
    if not rows: 
        return None
    df = (pd.DataFrame(rows).groupby("month",as_index=False).max()
            .sort_values("month"))
    return df

def main(year):
    ovr = Path(f"overrides/expenses_{year}.csv")
    if ovr.exists():
        df = pd.read_csv(ovr, parse_dates=["month"])
    else:
        df = parse_visuals_fallback(year)
        if df is None or df.empty:
            raise SystemExit("No override and could not parse visuals.")
    # ensure floats
    for c in ("completed","projected"): df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    write_gold(df, year)
    logging.info(f"[gold] rebuilt from {'override' if ovr.exists() else 'visuals fallback'}")

if __name__ == "__main__":
    year = get_year()
    main(year)
