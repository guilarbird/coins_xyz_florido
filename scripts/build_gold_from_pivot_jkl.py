#!/usr/bin/env python3
import duckdb, pandas as pd, re
from pathlib import Path
from datetime import date, datetime
import argparse
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB  = "warehouse/coins_xyz.duckdb"
SRC = "silver__analysis_analysis_2__analysis_analysis_2"   # Analysis tab (semicolon soup)
OUT = Path("gold"); OUT.mkdir(exist_ok=True)

def get_year():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Reporting year")
    args = parser.parse_args()
    if args.year:
        return args.year
    if os.getenv("REPORTING_YEAR"):
        return int(os.getenv("REPORTING_YEAR"))
    return datetime.now().year

def parse_brl(x: str) -> float | None:
    if x is None: return None
    s = str(x)
    s = re.sub(r"[^0-9,.\-]", "", s)
    if s in ("", ".", "-", "--"): return None
    s = s.replace(".", "").replace(",", ".")
    try: return float(s)
    except: return None

# 2025-jan. / 2025-fev. ... -> first day-of-month
PT_MON = {"jan.":1,"fev.":2,"mar.":3,"abr.":4,"mai.":5,"jun.":6,"jul.":7,"ago.":8,"set.":9,"out.":10,"nov.":11,"dez.":12}
def parse_pt_ym(tok: str) -> date | None:
    s = tok.strip().lower()
    m = re.match(r"^(\d{4})[-/\s]?([a-z]{3}\.)$", s)
    if not m: return None
    y = int(m.group(1)); mon = PT_MON.get(m.group(2))
    if not mon: return None
    return date(y, mon, 1)

def pick_rich_cell(row_vals):
    cells = [str(v) for v in row_vals if pd.notna(v) and str(v) != "None"]
    return max(cells, key=lambda s: s.count(";")) if cells else None

def read_lines_from_source() -> list[list[str]]:
    with duckdb.connect(DB) as con:
        df = con.execute(f"SELECT * FROM {SRC}").df()
    lines = []
    for _, row in df.iterrows():
        cell = pick_rich_cell(row.values)
        if not cell: continue
        toks = [p.strip() for p in cell.split(";")]
        lines.append(toks)
    return lines

def extract_pivot_JKL(lines: list[list[str]]) -> pd.DataFrame:
    # find header row that contains the three columns we want
    hdr_idx = None
    col_pos = {}
    for i, toks in enumerate(lines[:80]):  # search early region
        low = [t.lower() for t in toks]
        if {"completed","projected","total geral"} <= set(low):
            hdr_idx = i
            for j, t in enumerate(low):
                if t in ("completed","projected","total geral"):
                    col_pos[t] = j
            break
    if hdr_idx is None:
        raise SystemExit("Header with 'Completed; Projected; Total geral' not found.")

    # rows start right after header; stop at 'Total geral' subtotal row
    rows = []
    for toks in lines[hdr_idx+1:]:
        low = [t.lower() for t in toks]
        # explicit stop when we hit the block's grand total
        if len(low) >= 1 and low[0].startswith("total geral"):
            break
        if not toks or len(toks) < max(col_pos.values())+1:
            continue
        ym = parse_pt_ym(toks[0])
        if ym is None:
            continue
        comp = parse_brl(toks[col_pos["completed"]])
        proj = parse_brl(toks[col_pos["projected"]])

        # None -> 0 so months appear consistently (and match your CSVs)
        comp = comp or 0.0
        proj = proj or 0.0

        rows.append({"month": ym, "Completed": comp, "Projected": proj})

    return pd.DataFrame(rows).sort_values("month")

def main(year):
    lines = read_lines_from_source()
    table = extract_pivot_JKL(lines)

    # 1) Long canonical
    canon = pd.concat([
        table[["month","Completed"]].rename(columns={"Completed":"amount"})
              .assign(status="Completed"),
        table[["month","Projected"]].rename(columns={"Projected":"amount"})
              .assign(status="Projected"),
    ], ignore_index=True)
    canon["tx_date"] = canon["month"]  # monthly series → tx_date = month

    # 2) Bars per month/status
    bars = canon.groupby(["month","status"], as_index=False)["amount"].sum().rename(columns={"amount":"total"})

    # 3) Cumulative year
    cy = table[table["month"].dt.year==year].copy()
    # make sure both columns exist as Series
    if "Completed" not in cy: cy["Completed"] = pd.Series(dtype=float)
    if "Projected" not in cy: cy["Projected"] = pd.Series(dtype=float)
    cy = cy.sort_values("month")
    cum = pd.DataFrame({
        "month": cy["month"],
        "cum_completed": cy["Completed"].cumsum(),
        "cum_total": (cy["Completed"] + cy["Projected"]).cumsum(),
    })

    # Write parquet + csv for convenience
    canon[["tx_date","month","amount","status"]].assign(category=None, description=None, source=SRC)\
        .to_parquet(OUT/"expenses_canon.parquet", index=False)
    bars.to_parquet(OUT/"expenses_per_month_status.parquet", index=False)
    cum.to_parquet(OUT/f"expenses_cumulative_{year}.parquet", index=False)

    bars.to_csv(OUT/"expenses_per_month_status.csv", index=False)
    cum.to_csv(OUT/f"expenses_cumulative_{year}.csv", index=False)

    # Sanity print
    logging.info("[OK] gold rebuilt from Analysis pivot (J:K:L).")
    logging.info(bars.to_string(index=False))

if __name__ == "__main__":
    year = get_year()
    main(year)
