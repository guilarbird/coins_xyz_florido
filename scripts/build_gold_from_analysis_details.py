#!/usr/bin/env python3
import duckdb, pandas as pd, re
from pathlib import Path
from datetime import datetime

DB = "warehouse/coins_xyz.duckdb"
OUT = "gold"
Path(OUT).mkdir(exist_ok=True)

DETAILS_VIEW = "silver__analysis_analysis_5__analysis_analysis_5"   # Date;Entity;Amount (Sum)
STATUS_VIEW  = "silver__analysis_analysis_2__analysis_analysis_2"    # Status; Completed; Projected; Grand

num_rx = re.compile(r"[0-9.,-]")

def brl(x):
    if x is None or (isinstance(x, float) and pd.isna(x)): return None
    s = "".join(ch for ch in str(x) if num_rx.match(ch))
    if s in ("", ".", ",", "-", "--"): return None
    return float(s.replace(".", "").replace(",", "."))

def is_date(s):
    return isinstance(s, str) and re.match(r"^\s*\d{1,2}[-/]\d{1,2}[-/]\d{4}\s*$", s or "")

def month_from(s):
    d = datetime.strptime(s.strip().replace("/", "-"), "%d-%m-%Y").date()
    return d.replace(day=1)

def richest_cell(row_vals):
    cells = [str(v) for v in row_vals if pd.notna(v) and str(v) != "None"]
    return max(cells, key=lambda s: s.count(";")) if cells else None

con = duckdb.connect(DB)

def parse_completed_from_details(view: str) -> pd.DataFrame:
    df = con.execute(f"SELECT * FROM {view}").df()
    out = []
    for _, row in df.iterrows():
        cell = richest_cell(row.values)
        if not cell: continue
        toks = [t.strip() for t in cell.split(";")]
        if not toks or not is_date(toks[0]): continue
        # pick the LAST numeric token on the row as the amount (matches “Grand/Amount (Sum)” column)
        nums = [brl(t) for t in toks[1:] if brl(t) is not None]
        if not nums: continue
        amt = nums[-1]
        out.append({
            "tx_date": month_from(toks[0]),
            "month":   month_from(toks[0]),
            "amount":  amt,
            "status":  "Completed",
            "source":  view
        })
    print(f"[details] {view}: parsed {len(out)} Completed rows")
    return pd.DataFrame(out)

def parse_projected_from_status(view: str) -> pd.DataFrame:
    df = con.execute(f"SELECT * FROM {view}").df()
    # Flatten rows into token lines
    lines = []
    for _, row in df.iterrows():
        cell = richest_cell(row.values)
        if cell: lines.append([t.strip() for t in cell.split(";")])

    out = []
    i = 0
    while i < len(lines):
        low = [t.lower() for t in lines[i]]
        if "status" in low and "completed" in low:
            idx_completed = low.index("completed")
            idx_projected = low.index("projected") if "projected" in low else None
            # next row should be "Date; Amount ..."
            i += 2
            while i < len(lines) and lines[i] and is_date(lines[i][0]):
                d = month_from(lines[i][0])
                if idx_projected is not None and idx_projected < len(lines[i]):
                    val = brl(lines[i][idx_projected])
                    if val is not None:
                        out.append({
                            "tx_date": d, "month": d,
                            "amount": val, "status": "Projected", "source": view
                        })
                i += 1
            continue
        i += 1
    print(f"[status]  {view}: parsed {len(out)} Projected rows")
    return pd.DataFrame(out)

completed = parse_completed_from_details(DETAILS_VIEW)
projected = parse_projected_from_status(STATUS_VIEW)

canon = pd.concat([completed, projected], ignore_index=True) if not completed.empty or not projected.empty else pd.DataFrame(columns=["tx_date","month","amount","status","source"])
canon.to_parquet(f"{OUT}/expenses_canon.parquet", index=False)

# Bars (month, status)
bars = (canon.groupby(["month","status"], as_index=False)["amount"].sum()
             .rename(columns={"amount":"total"})
             .sort_values(["month","status"]))
if not bars.empty:
    bars["total"] = bars["total"].round(2)
    bars.to_csv(f"{OUT}/expenses_per_month_status.csv", index=False)
    bars.to_parquet(f"{OUT}/expenses_per_month_status.parquet", index=False)

# Cumulative 2025
c2025 = canon[canon["month"].apply(lambda d: getattr(d, "year", 0) == 2025)]
if not c2025.empty:
    m = (c2025.pivot_table(index="month", columns="status", values="amount", aggfunc="sum")
              .fillna(0).sort_index())
    comp = m["Completed"] if "Completed" in m.columns else pd.Series(0.0, index=m.index)
    proj = m["Projected"] if "Projected" in m.columns else pd.Series(0.0, index=m.index)
    out = pd.DataFrame({
        "month":        m.index,
        "cum_completed": comp.cumsum().values,
        "cum_total":     (comp + proj).cumsum().values
    })
    out.to_csv(f"{OUT}/expenses_cumulative_2025.csv", index=False, float_format="%.2f")
    out.to_parquet(f"{OUT}/expenses_cumulative_2025.parquet", index=False)

print("[gold] rebuilt from details (Completed) + status (Projected)")
