#!/usr/bin/env python3
import pandas as pd, duckdb, os, re
from pathlib import Path
OUT = Path("gold"); OUT.mkdir(exist_ok=True)
OVR = Path("overrides/expenses_2025.csv")
DB  = "warehouse/coins_xyz.duckdb"

def write_gold(df):
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

    # cumulative 2025
    y2025 = df[df["month"].dt.year==2025].copy()
    y2025["cum_completed"] = y2025["completed"].cumsum()
    y2025["cum_total"]     = (y2025["completed"] + y2025["projected"]).cumsum()
    y2025[["month","cum_completed","cum_total"]].to_parquet(OUT/"expenses_cumulative_2025.parquet", index=False)
    y2025[["month","cum_completed","cum_total"]].to_csv(OUT/"expenses_cumulative_2025.csv", index=False)

def parse_visuals_fallback():
    # very light fallback: try to read the “status” pivot from analysis_table_1
    con = duckdb.connect(DB)
    try:
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
            m = re.search(r"2025-(\w+)", " ".join(toks)).group(1).strip(".").lower()
            month = pd.Timestamp(year=2025, month=month_map[m], day=1)
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

def main():
    if OVR.exists():
        df = pd.read_csv(OVR, parse_dates=["month"])
    else:
        df = parse_visuals_fallback()
        if df is None or df.empty:
            raise SystemExit("No override and could not parse visuals.")
    # ensure floats
    for c in ("completed","projected"): df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    write_gold(df)
    print("[gold] rebuilt from", "override" if OVR.exists() else "visuals fallback")

if __name__ == "__main__":
    main()
