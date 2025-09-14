#!/usr/bin/env python3
import sys, re, hashlib, numpy as np, pandas as pd
from pathlib import Path

XLS = sys.argv[1] if len(sys.argv)>1 else "01 Expenses Management.xlsx"
OUT = Path("gold"); OUT.mkdir(exist_ok=True)

def brl(x):
    if x is None or (isinstance(x,float) and np.isnan(x)): return None
    s=re.sub(r"[^0-9,.\-]","",str(x)).replace(".","").replace(",",".")
    try: return float(s) if s not in ("",".","-","--") else None
    except: return None

def norm_cols(df):
    m={}
    for c in df.columns:
        cl=c.strip().lower()
        if cl in ("date","data","dt"): m[c]="tx_date"
        elif "amount" in cl or "valor" in cl: m[c]="amount_brl"
        elif "status" in cl: m[c]="status"
        elif "category" in cl or "categoria" in cl: m[c]="category"
        elif "merchant" in cl: m[c]="merchant"
        elif "description" in cl or "descri" in cl or "memo" in cl: m[c]="description"
        elif "entity" in cl: m[c]="entity"
        elif "bank" in cl: m[c]="bank"
        elif "type" in cl: m[c]="payment_type"
        else: m[c]=c
    df=df.rename(columns=m)
    if "tx_date" in df.columns:
        df["tx_date"]=pd.to_datetime(df["tx_date"], errors="coerce", dayfirst=True)
    if "amount_brl" in df.columns:
        df["amount_brl"]=df["amount_brl"].apply(brl)
    return df

def hash_id(row):
    key="|".join(str(row.get(k,"")) for k in
        ["tx_date","amount_brl","description","merchant","entity","bank"])
    return hashlib.sha1(key.encode()).hexdigest()[:16]

def main():
    if not Path(XLS).exists():
        raise SystemExit(f"Workbook not found: {XLS}")
    sheets=pd.read_excel(XLS, sheet_name=None, dtype=str)
    frames=[]
    for name,df in sheets.items():
        df=norm_cols(df)
        if {"tx_date","amount_brl"} <= set(df.columns):
            keep=["tx_date","amount_brl","status","category","description",
                  "merchant","entity","bank","payment_type"]
            df=df[keep].copy()
            df["source"]=name
            frames.append(df)

    if not frames: raise SystemExit("No detail-like sheets (date+amount).")
    tx=pd.concat(frames, ignore_index=True).dropna(subset=["tx_date","amount_brl"])
    tx["status"]=tx["status"].fillna("Completed").str.title()
    tx["month"]=tx["tx_date"].dt.to_period("M").dt.to_timestamp()
    tx["tx_id"]=tx.apply(hash_id, axis=1)

    cols=["tx_id","tx_date","month","amount_brl","status","category",
          "description","merchant","entity","bank","payment_type","source"]
    tx[cols].to_parquet(OUT/"expenses_tx.parquet", index=False)

    # Derived monthly views for convenience
    bars=(tx.groupby(["month","status"],as_index=False)["amount_brl"].sum()
            .rename(columns={"amount_brl":"total"}))
    bars.to_parquet(OUT/"expenses_per_month_status.parquet", index=False)

    m2025=tx[tx["month"].dt.year==2025]
    if not m2025.empty:
        p=(m2025.pivot_table(index="month",columns="status",values="amount_brl",aggfunc="sum")
              .fillna(0).sort_index())
        p["cum_completed"]=p.get("Completed",0).cumsum()
        p["cum_total"]=(p.get("Completed",0)+p.get("Projected",0)).cumsum()
        p.reset_index()[["month","cum_completed","cum_total"]].to_parquet(
            OUT/"expenses_cumulative_2025.parquet", index=False)

    print("[gold] wrote gold/expenses_tx.parquet and updated monthly parquet files.")
if __name__=="__main__": main()
