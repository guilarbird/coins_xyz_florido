#!/usr/bin/env python3
import duckdb, pandas as pd, re
from pathlib import Path
from datetime import datetime
import argparse
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

DETAILS_VIEW = "silver__analysis_analysis_5__analysis_analysis_5"   # Date;Entity;Amount (Sum)
STATUS_VIEW  = "silver__analysis_analysis_2__analysis_analysis_2"   # Status; Completed; Projected; Grand

rx_num = re.compile(r"[0-9.,-]")
def brl(x):
    if x is None or (isinstance(x,float) and pd.isna(x)): return None
    s = "".join(ch for ch in str(x) if rx_num.match(ch))
    if s in ("", ".", ",", "-", "--"): return None
    return float(s.replace(".","").replace(",","."))
def is_date(s):
    return isinstance(s,str) and re.match(r"^\s*\d{1,2}[-/]\d{1,2}[-/]\d{4}\s*$", s or "")
def month_from(s):
    d = datetime.strptime(s.strip().replace("/","-"), "%d-%m-%Y").date()
    return d.replace(day=1)
def richest(row_vals):
    cells=[str(v) for v in row_vals if pd.notna(v) and str(v)!="None"]
    return max(cells, key=lambda s:s.count(";")) if cells else None

def main(year):
    with duckdb.connect(DB) as con:
        # ---------- COMPLETED from details (analysis_5)
        completed = pd.DataFrame(columns=["month","completed"])
        try:
            det_df = con.execute(f"SELECT * FROM {DETAILS_VIEW}").df()
            det_rows=[]
            for _,row in det_df.iterrows():
                cell = richest(row.values)
                if not cell: continue
                toks = [t.strip() for t in cell.split(";")]
                if not toks or not is_date(toks[0]): continue
                nums = [brl(t) for t in toks[1:] if brl(t) is not None]
                if not nums: continue
                amt = nums[-1]                # last numeric token
                m = month_from(toks[0])
                det_rows.append({"month": m, "amount": amt})
            if det_rows:
                completed = (pd.DataFrame(det_rows)
                             .groupby("month", as_index=False)["amount"].sum()
                             .rename(columns={"amount":"completed"}))
            logging.info(f"[details] Completed rows parsed: {len(det_rows)} | months={len(completed)}")
        except Exception as e:
            logging.warning(f"[details] WARN: {e}")

        # ---------- PROJECTED/COMPLETED by scanning ALL Status blocks (analysis_2)
        blocks = []
        try:
            st_df = con.execute(f"SELECT * FROM {STATUS_VIEW}").df()
            lines=[]
            for _,row in st_df.iterrows():
                cell=richest(row.values)
                if cell: lines.append([t.strip() for t in cell.split(";")])

            i=0
            while i < len(lines):
                low=[t.lower() for t in lines[i]]
                if "status" in low and "completed" in low:
                    idx_completed = low.index("completed")
                    idx_projected = low.index("projected") if "projected" in low else None
                    rows=[]; j=i+2
                    while j < len(lines) and lines[j] and is_date(lines[j][0]):
                        d  = month_from(lines[j][0])
                        comp = brl(lines[j][idx_completed]) if idx_completed < len(lines[j]) else None
                        proj = brl(lines[j][idx_projected]) if (idx_projected is not None and idx_projected < len(lines[j])) else None
                        rows.append({"month": d, "completed": comp, "projected": proj})
                        j += 1
                    if rows:
                        dfb = (pd.DataFrame(rows)
                               .groupby("month", as_index=False)
                               .agg({"completed":"sum","projected":"sum"}))
                        blocks.append(dfb)
                    i = j; continue
                i += 1
            logging.info(f"[status] Found status blocks: {len(blocks)}")
        except Exception as e:
            logging.warning(f"[status] WARN: {e}")

        # ---------- Choose best Status block (vs details completed), or widest if no details
        chosen = None
        if blocks:
            if completed.empty:
                chosen = max(blocks, key=lambda d: len(d))
                logging.info("[status] No details found → choosing the widest status block")
            else:
                best = None; best_err = None
                for b in blocks:
                    merged = completed.merge(b[["month","completed"]].rename(columns={"completed":"b_completed"}),
                                             on="month", how="inner")
                    if merged.empty:
                        err = float("inf")
                    else:
                        diff  = (merged["completed"] - merged["b_completed"]).abs()
                        scale = merged["completed"].abs().clip(lower=1.0)
                        err   = float((diff/scale).mean())
                    if best_err is None or err < best_err:
                        best_err = err; best = b
                chosen = best
                logging.info(f"[status] Chosen block relative error vs details: {best_err:.6f}" if best_err is not None else "[status] Chosen block")

        # ---------- Build month-level bars (Completed, Projected)
        proj = pd.DataFrame(columns=["month","projected"])
        comp_from_status = pd.DataFrame(columns=["month","completed"])
        if chosen is not None:
            if "projected" in chosen.columns:
                proj = chosen[["month","projected"]].copy().fillna(0.0)
            if "completed" in chosen.columns:
                comp_from_status = chosen[["month","completed"]].copy().fillna(0.0)

        if completed.empty:
            completed = comp_from_status.copy()  # fallback

        bars = completed.merge(proj, on="month", how="outer").fillna(0.0)
        if "completed" not in bars.columns: bars["completed"]=0.0
        if "projected" not in bars.columns: bars["projected"]=0.0

        bars_long = pd.concat([
            pd.DataFrame({"month":bars["month"], "status":"Completed", "total":bars["completed"]}),
            pd.DataFrame({"month":bars["month"], "status":"Projected", "total":bars["projected"]}),
        ], ignore_index=True)
        bars_long = bars_long[(bars_long["total"]!=0)].sort_values(["month","status"])
        bars_long["total"] = bars_long["total"].round(2)
        bars_long.to_csv(f"{OUT}/expenses_per_month_status.csv", index=False)
        bars_long.to_parquet(f"{OUT}/expenses_per_month_status.parquet", index=False)

        # ---------- Canon (month-level) for downstream simplicity
        canon = pd.concat([
            pd.DataFrame({"tx_date":bars["month"], "month":bars["month"], "amount":bars["completed"], "status":"Completed", "source":DETAILS_VIEW if not completed.empty else STATUS_VIEW}),
            pd.DataFrame({"tx_date":bars["month"], "month":bars["month"], "amount":bars["projected"], "status":"Projected", "source":STATUS_VIEW}),
        ], ignore_index=True)
        canon.to_parquet(f"{OUT}/expenses_canon.parquet", index=False)

        # ---------- Cumulative year
        y = bars[(bars["month"].apply(lambda d: getattr(d, "year", 0))==year)].sort_values("month")
        if not y.empty:
            comp = pd.Series(y["completed"].values, index=y["month"])
            proj = pd.Series(y["projected"].values, index=y["month"])
            out = pd.DataFrame({
                "month":        y["month"].values,
                "cum_completed": comp.cumsum().round(2).values,
                "cum_total":     (comp+proj).cumsum().round(2).values,
            })
            out.to_csv(f"{OUT}/expenses_cumulative_{year}.csv", index=False)
            out.to_parquet(f"{OUT}/expenses_cumulative_{year}.parquet", index=False)

        # ---------- Debug all status blocks (optional inspection)
        if blocks:
            dbg=[]
            for k,b in enumerate(blocks):
                t=b.copy(); t["block_id"]=k; dbg.append(t)
            pd.concat(dbg, ignore_index=True).to_csv(f"{OUT}/debug_status_blocks.csv", index=False)

        logging.info("[gold] rebuilt (Completed from details or fallback-to-status; Projected from chosen status)")

if __name__ == "__main__":
    year = get_year()
    main(year)
