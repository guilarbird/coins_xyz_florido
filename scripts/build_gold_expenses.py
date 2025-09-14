#!/usr/bin/env python3
import pandas as pd, duckdb, yaml, re
from pathlib import Path

SILVER = Path("silver/expenses_details.parquet")
GOLD = Path("gold")
CFG = Path("configs/category_rules.yaml")
GOLD.mkdir(exist_ok=True, parents=True)

def load_rules():
    if CFG.exists():
        with open(CFG, "r") as f:
            return yaml.safe_load(f) or {}
    return {"map": {}, "status_map": {}}

def apply_category_rules(df: pd.DataFrame, rules: dict) -> pd.Series:
    cat = df["category"].astype("string").fillna("")
    desc = df["description"].astype("string").fillna("")
    out = cat.copy()
    for patt, catname in (rules.get("map") or {}).items():
        rx = re.compile(patt, re.I)
        mask = desc.str.contains(rx) | cat.str.contains(rx)
        out = out.mask(mask, catname)
    return out.fillna("uncategorized").replace("", "uncategorized")

def coerce_status(df: pd.DataFrame, rules: dict) -> pd.Series:
    s = df.get("status", pd.Series(index=df.index, dtype="string")).astype("string").fillna("")
    mapped = s.str.lower().map(rules.get("status_map", {})).fillna(s)
    mapped = mapped.str.capitalize()
    return mapped.where(mapped.isin(["Completed","Projected"]), "Completed")

def month_floor(s):
    s = pd.to_datetime(s, errors="coerce")
    return pd.to_datetime(s.dt.to_period("M").dt.start_time)

def brl_num(x: str) -> float:
    x = re.sub(r"[^0-9,.\-]","", x or "")
    x = x.replace(".","").replace(",",".")
    try: return round(float(x),2)
    except: return 0.0

def build_gold():
    if not SILVER.exists():
        raise SystemExit("Run scripts/build_silver_details.py first (silver/expenses_details.parquet missing).")
    df = pd.read_parquet(SILVER)
    rules = load_rules()
    df = df.copy()
    df["status"] = coerce_status(df, rules)
    df["category"] = apply_category_rules(df, rules)
    df["month"] = month_floor(df["date"])
    df["tx_date"] = pd.to_datetime(df["date"]).dt.date
    df["amount_brl"] = pd.to_numeric(df["amount_brl"], errors="coerce").round(2)

    tx = df[["tx_date","month","amount_brl","status","category","description","bank","entity","source_file"]].dropna(subset=["month","amount_brl"])
    tx = tx.rename(columns={"amount_brl":"amount"})
    tx.to_parquet(GOLD/"expenses_tx.parquet", index=False)

    bars = (tx.groupby(["month","status"], as_index=False)["amount"].sum()
              .sort_values(["month","status"]))
    bars.to_parquet(GOLD/"expenses_per_month_status.parquet", index=False)
    bars.to_csv(GOLD/"expenses_per_month_status.csv", index=False)

    m2025 = tx[tx["month"].dt.year==2025]
    if not m2025.empty:
        p = (m2025.pivot_table(index="month", columns="status", values="amount", aggfunc="sum")
                     .fillna(0).sort_index())
        p["cum_completed"] = p.get("Completed",0).cumsum()
        p["cum_total"]     = (p.get("Completed",0)+p.get("Projected",0)).cumsum()
        p.reset_index()[["month","cum_completed","cum_total"]].to_parquet(GOLD/"expenses_cumulative_2025.parquet", index=False)
        p.reset_index()[["month","cum_completed","cum_total"]].to_csv(GOLD/"expenses_cumulative_2025.csv", index=False)

    status_csv = next((p for p in Path("raw_data").glob("*Analysis*2*.csv")), None)
    if status_csv:
        try:
            raw = pd.read_csv(status_csv)
            lines=[]
            for _,r in raw.iterrows():
                cells=[str(v) for v in r.values if pd.notna(v) and str(v)!="None"]
                if not cells: continue
                cell=max(cells, key=lambda s:s.count(";"))
                toks=[t.strip() for t in cell.split(";")]
                lines.append(toks)
            head=[t.lower() for t in lines[0]]
            iC = head.index("completed") if "completed" in head else None
            iP = head.index("projected") if "projected" in head else None
            data=[]
            for toks in lines[2:]:
                if not toks or not re.match(r"\d{2}[-/]\d{2}[-/]\d{4}", toks[0]): continue
                m = pd.to_datetime(toks[0].replace("/","-"), dayfirst=True).to_period("M").start_time
                if iC is not None and iC<len(toks): data.append([m,"Completed",brl_num(toks[iC])])
                if iP is not None and iP<len(toks): data.append([m,"Projected",brl_num(toks[iP])])
            ref = pd.DataFrame(data, columns=["month","status","total"]).groupby(["month","status"],as_index=False).sum()
            mine = bars.rename(columns={"amount":"total"})
            rec = mine.merge(ref, on=["month","status"], how="outer", suffixes=("_gold","_pivot")).fillna(0)
            rec["diff"] = (rec["total_gold"] - rec["total_pivot"]).round(2)
            rec.sort_values(["month","status"]).to_csv(GOLD/"reconciliation_report.csv", index=False)
            print("[reconcile] wrote gold/reconciliation_report.csv")
        except Exception as e:
            print(f"[reconcile] skipped pivot reconciliation: {e}")

    con = duckdb.connect("warehouse/coins_xyz.duckdb")
    con.execute("""
    CREATE OR REPLACE VIEW gold__expenses_tx AS SELECT * FROM read_parquet('gold/expenses_tx.parquet');
    CREATE OR REPLACE VIEW gold__expenses_per_month_status AS SELECT * FROM read_parquet('gold/expenses_per_month_status.parquet');
    """)
    if (GOLD/"expenses_cumulative_2025.parquet").exists():
        con.execute("CREATE OR REPLACE VIEW gold__expenses_cumulative_2025 AS SELECT * FROM read_parquet('gold/expenses_cumulative_2025.parquet');")
        print("[gold] wrote gold/*.parquet & registered views: gold__expenses_tx, gold__expenses_per_month_status, gold__expenses_cumulative_2025")
    else:
        print("[gold] wrote gold/*.parquet & registered views: gold__expenses_tx, gold__expenses_per_month_status")

if __name__ == "__main__":
    build_gold()
