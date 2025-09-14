#!/usr/bin/env python3
import pandas as pd, re
from pathlib import Path

RAW = Path("raw_data")
OUT = Path("silver/expenses_details.parquet")
OUT.parent.mkdir(exist_ok=True, parents=True)

def slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = s.replace("(sum)","")
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s or "col"

def parse_amount_brl(x):
    if pd.isna(x): return None
    s = str(x)
    s = re.sub(r"[^0-9,.\-]", "", s)
    s = s.replace(".", "").replace(",", ".")
    try: return round(float(s), 2)
    except: return None

def maybe_semicolon_table(df: pd.DataFrame) -> bool:
    return df.shape[1] == 2 and {"table","1"}.issubset(set(df.columns.str.lower()))

def split_semicolon_sheet(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        parts = [str(v) for v in row.values if pd.notna(v) and str(v) != "None"]
        if not parts: continue
        line = ";".join(parts)
        toks = [p.strip() for p in line.split(";")]
        rows.append(toks)
    if not rows: return pd.DataFrame()

    want = {"date","data","amount","valor","category","categoria","description","descricao","status","currency","bank","entity","merchant","name"}
    header_idx = 0
    for i, toks in enumerate(rows[:6]):
        low = [t.lower() for t in toks]
        if len(set(low) & want) >= 2: header_idx = i; break

    header = rows[header_idx]
    cols = [slug(c) if c else f"col_{i}" for i,c in enumerate(header)]
    width = len(cols)
    data = []
    for toks in rows[header_idx+1:]:
        t = (toks + [""]*width)[:width]
        data.append(t)
    out = pd.DataFrame(data, columns=cols).replace({"": pd.NA})
    return out.dropna(how="all")

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    colmap = {}
    new_cols = {}
    for c in df.columns:
        cl = c.lower()
        target_col = c
        if cl in ("date","data","dt","textdatevaluelefta110ddmmyyyy"): target_col = "date"
        elif cl in ("amount","valor","value") or "vol brl" in cl: target_col = "amount"
        elif re.search("category|categoria", cl): target_col = "category"
        elif re.search("description|descricao|memo|detail|name|merchant", cl): target_col = "description"
        elif "status" in cl: target_col = "status"
        elif re.search("currency|moeda", cl): target_col = "currency"
        elif re.search("bank|banco", cl): target_col = "bank"
        elif re.search("entity|entidade", cl): target_col = "entity"
        
        if target_col in new_cols.values():
            i = 1
            while f"{target_col}_{i}" in new_cols.values():
                i += 1
            target_col = f"{target_col}_{i}"
        new_cols[c] = target_col

    df = df.rename(columns=new_cols)
    if "date" in df: df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
    df["amount_brl"] = df.get("amount", pd.Series(index=df.index)).apply(parse_amount_brl)
    keep = ["date","amount_brl","category","description","status","currency","bank","entity"]
    for k in keep:
        if k not in df.columns: df[k] = pd.NA
    
    final_cols = [c for c in keep if c in df.columns]
    return df[final_cols].dropna(how="all")

def load_one_csv(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, sep=';')
    except Exception:
        df = pd.read_csv(path, sep=';', encoding="latin1")
    return normalize_cols(df)

def main():
    frames = []
    files_to_process = [
        "expenses.csv",
        "credit_card_history.csv"
    ]
    for filename in files_to_process:
        path = RAW / filename
        if not path.exists():
            print(f"[silver] {path} not found. Skipping.")
            continue
        try:
            d = load_one_csv(path)
            if not d.empty:
                d["source_file"] = path.name
                frames.append(d)
                print(f"[silver] parsed {path.name}: rows={len(d)}")
        except Exception as e:
            print(f"[silver] skip {path.name}: {e}")

    if not frames:
        print("[silver] NO INPUT ROWS in specified CSV files"); return
    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["date","amount_brl"], how="all")
    out.to_parquet(OUT, index=False)
    print(f"[silver] wrote {OUT} rows={len(out)} cols={list(out.columns)}")

if __name__ == "__main__":
    main()
