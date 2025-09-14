#!/usr/bin/env python3
from pathlib import Path
import duckdb, pandas as pd, re, numpy as np

DB = "warehouse/coins_xyz.duckdb"
OUT_DIR = Path("silver/expenses_history_parsed")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT = OUT_DIR / "expenses_history_parsed.parquet"

RAW_CANDIDATES = [
    "silver__history_table_1__history_table_1",
    "silver__credit_card_history_table_1__credit_card_history_table_1",
]

def slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = s.replace("(sum)", "")
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s

def parse_amount_brl(x: str) -> float | None:
    if x is None or (isinstance(x, float) and pd.isna(x)): return None
    s = str(x)
    s = re.sub(r"[^0-9,.\-]", "", s)  # só dígitos, vírgula, ponto, sinal
    s = s.replace(".", "")            # remove milhares
    s = s.replace(",", ".")           # vírgula decimal → ponto
    try:
        return float(s) if s not in ("", ".", "-", "--") else None
    except Exception:
        return None

def pick_header(lines):
    """Escolhe o header olhando as 10 primeiras linhas."""
    want = {"date","data","amount","valor","category","categoria","description","descricao","currency","bank","status","entity"}
    hdr_idx, hdr = 0, lines[0] if lines else []
    for i, toks in enumerate(lines[:10]):
        low = [t.lower() for t in toks]
        if len(set(low) & want) >= 2 and len(toks) >= 2:
            return i, toks
    return hdr_idx, hdr

def parse_one_view(df: pd.DataFrame, viewname: str) -> pd.DataFrame:
    # Constrói linhas a partir da célula com mais ';' em cada linha (não concatena colunas!)
    rows = []
    for _, row in df.iterrows():
        cells = [str(v) for v in row.values if pd.notna(v) and str(v) != "None"]
        if not cells:
            continue
        candidate = max(cells, key=lambda s: s.count(";"))
        toks = [p.strip() for p in candidate.split(";")]
        rows.append(toks)

    if not rows:
        print(f"[parse] {viewname}: 0 linhas úteis")
        return pd.DataFrame()

    hdr_idx, header_tokens = pick_header(rows)
    cols = [slug(t) if t else f"col_{i}" for i, t in enumerate(header_tokens)]
    width = len(cols)
    data = []
    for toks in rows[hdr_idx+1:]:
        if len(toks) < width:
            toks = toks + [""]*(width - len(toks))
        elif len(toks) > width:
            toks = toks[:width]
        data.append(toks)

    parsed = pd.DataFrame(data, columns=cols)
    parsed.replace("", pd.NA, inplace=True)
    # Renomeia nomes esquisitos para canônico
    colmap = {}
    for c in parsed.columns:
        cl = c.lower()
        if cl in ("date","data","dt","textdatevaluelefta110ddmmyyyy"): colmap[c] = "date"
        elif cl in ("amount","valor","value"):                           colmap[c] = "amount"
        elif re.search(r"category|categoria", cl):                       colmap[c] = "category"
        elif re.search(r"description|descricao|memo|detail", cl):        colmap[c] = "description"
        else:                                                            colmap[c] = c
    parsed.rename(columns=colmap, inplace=True)

    # Tipagem
    if "date" in parsed.columns:
        parsed["date"] = pd.to_datetime(parsed["date"], errors="coerce", dayfirst=True)
    if "amount" in parsed.columns:
        parsed["amount"] = parsed["amount"].apply(parse_amount_brl)

    # Mantém linhas com pelo menos date OU amount
    keep = None
    if "amount" in parsed.columns:
        keep = parsed["amount"].notna()
    if "date" in parsed.columns:
        keep = keep | parsed["date"].notna() if keep is not None else parsed["date"].notna()
    parsed = parsed[keep] if keep is not None else parsed
    print(f"[parse] {viewname}: header={cols} → linhas úteis={len(parsed)}")
    return parsed

def main():
    con = duckdb.connect(DB)
    tables = {t[0] for t in con.execute("SHOW TABLES").fetchall()}

    frames = []
    for v in RAW_CANDIDATES:
        if v in tables:
            df = con.execute(f"SELECT * FROM {v}").df()
            frames.append(parse_one_view(df, v))
        else:
            print(f"[parse] ausente: {v}")

    if not frames:
        raise SystemExit("Nenhuma view candidata encontrada.")

    parsed = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    # drop linhas totalmente vazias e duplicatas
    parsed = parsed.dropna(how="all").drop_duplicates()
    parsed.to_parquet(OUT, index=False)
    print(f"[parse] wrote {OUT} cols={list(parsed.columns)} rows={len(parsed)}")

if __name__ == "__main__":
    main()
