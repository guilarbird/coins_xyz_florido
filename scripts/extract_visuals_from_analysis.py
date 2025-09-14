#!/usr/bin/env python3
import duckdb, pandas as pd, re
from datetime import datetime
from pathlib import Path
import argparse
import os

DB="warehouse/coins_xyz.duckdb"
OUT="gold"
Path(OUT).mkdir(exist_ok=True)

def get_year():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Reporting year")
    args = parser.parse_args()
    if args.year:
        return args.year
    if os.getenv("REPORTING_YEAR"):
        return int(os.getenv("REPORTING_YEAR"))
    return datetime.now().year

# main "Expenses" visual (+ optional card visual if you decide incluir)
VIEWS = [
  "silver__analysis_analysis_2__analysis_analysis_2",
  # "silver__credit_card___analysis_credit_card___analysis_2__credit_card___analysis_credit_card___analysis_2",
]

def parse_brl(x:str|float|None):
    if x is None or (isinstance(x,float) and pd.isna(x)): return None
    s=str(x)
    s=re.sub(r"[^0-9,.\-]","", s).replace(".","").replace(",",".")
    try:    return float(s) if s not in ("",".","-","--") else None
    except: return None

def richest_cell(row_vals):
    cells=[str(v) for v in row_vals if pd.notna(v) and str(v)!="None"]
    return max(cells, key=lambda s:s.count(";")) if cells else None

def extract_from_view(view)->pd.DataFrame:
    try:
        with duckdb.connect(DB) as con:
            df=con.execute(f"SELECT * FROM {view}").df()
    except Exception:
        return pd.DataFrame()

    # explode rows into token lists (by ;)
    lines=[]
    for _,row in df.iterrows():
        cell=richest_cell(row.values)
        if cell:
            lines.append([p.strip() for p in cell.split(";")])

    if not lines:
        return pd.DataFrame()

    # find header with 'Status; Completed; Projected; ...'
    status_idx=-1; status_cols={}
    for i,toks in enumerate(lines[:20]):
        low=[t.lower() for t in toks]
        if "status" in low and "completed" in low:
            status_idx=i
            for j, tok in enumerate(low):
                if tok in ("completed","projected","grand","grand total","grand_total"):
                    status_cols[tok.replace(" ","_")]=j
            break

    # date detection
    dt_rx=re.compile(r"^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}$")

    rows=[]
    # Walk the whole sheet, don’t break on non-date rows.
    # (Many Analysis exports have multiple blocks separated by subheaders.)
    for toks in lines:
        if not toks or not dt_rx.match(str(toks[0])):
            continue
        try:
            d = datetime.strptime(toks[0].replace("/","-"), "%d-%m-%Y").date().replace(day=1)
        except ValueError:
            continue

        if status_idx >= 0 and "completed" in status_cols:
            ci = status_cols["completed"]
            comp = parse_brl(toks[ci]) if ci < len(toks) else None
            if comp is not None:
                rows.append({"tx_date": d, "month": d, "amount": comp, "status": "Completed", "source": view})

            pi = status_cols.get("projected")
            if pi is not None and pi < len(toks):
                proj = parse_brl(toks[pi])
                if proj is not None:
                    rows.append({"tx_date": d, "month": d, "amount": proj, "status": "Projected", "source": view})
        else:
            # fallback: two-column "Date; Amount"
            amt = parse_brl(toks[1]) if len(toks)>1 else None
            if amt is not None:
                rows.append({"tx_date": d, "month": d, "amount": amt, "status": "Completed", "source": view})

    return pd.DataFrame(rows)

def main(year):
    frames=[extract_from_view(v) for v in VIEWS]
    canon=pd.concat([f for f in frames if not f.empty], ignore_index=True) if any([not f.empty for f in frames]) else pd.DataFrame()
    if canon.empty:
        print("[visuals] no rows found")
        return

    canon.to_parquet(f"{OUT}/expenses_canon.parquet", index=False)

    # bars
    bars = (canon.groupby(["month","status"], as_index=False)["amount"]
                 .sum().rename(columns={"amount":"total"}))
    # force 2 decimals for csv
    bars.to_csv(f"{OUT}/expenses_per_month_status.csv", index=False, float_format="%.2f")
    bars.to_parquet(f"{OUT}/expenses_per_month_status.parquet", index=False)

    # cumulative year
    cy = canon[canon["month"].apply(lambda d: getattr(d,"year",0)==year)]
    if not cy.empty:
        m = cy.pivot_table(index="month", columns="status", values="amount", aggfunc="sum").fillna(0).sort_index()
        m["cum_completed"] = m.get("Completed",0).cumsum()
        m["cum_total"]     = (m.get("Completed",0)+m.get("Projected",0)).cumsum()
        m.reset_index()[["month","cum_completed","cum_total"]].to_csv(f"{OUT}/expenses_cumulative_{year}.csv", index=False, float_format="%.2f")
        m.reset_index()[["month","cum_completed","cum_total"]].to_parquet(f"{OUT}/expenses_cumulative_{year}.parquet", index=False)

    print("[visuals] gold rebuilt from Analysis.")

if __name__ == "__main__":
    year = get_year()
    main(year)
