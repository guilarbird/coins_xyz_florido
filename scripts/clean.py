#!/usr/bin/env python3
import re, yaml, polars as pl
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BRONZE="bronze"; SILVER="silver"; LOGS="logs"
Path(LOGS).mkdir(exist_ok=True)

rules = yaml.safe_load(open("configs/cleaning_rules.yaml"))

date_re  = re.compile(rules["date_columns_regex"], re.IGNORECASE)
amt_re   = re.compile(rules["amount_columns_regex"], re.IGNORECASE)
currs    = tuple(rules.get("currency_symbols", []))
strip_th = rules.get("strip_thousand_sep", True)

def log_fail(p: Path, *errs):
    with open(f"{LOGS}/clean_failures.txt","a") as f:
        f.write(f"[{datetime.now().isoformat(timespec='seconds')}] {p}\n")
        for e in errs:
            if e: f.write(f"  - {type(e).__name__}: {e}\n")
        f.write("\n")

def read_parquet_robust(p: Path) -> pl.DataFrame:
    # 1) PyArrow canônico (o mais tolerante)
    try:
        import pyarrow.parquet as pq
        tbl = pq.read_table(p)
        return pl.from_arrow(tbl)
    except BaseException as e_pa:
        # 2) Polars com engine PyArrow
        try:
            return pl.read_parquet(p, use_pyarrow=True)
        except BaseException as e_pl_py:
            # 3) Polars nativo
            try:
                return pl.read_parquet(p)
            except BaseException as e_pl:
                log_fail(p, e_pa, e_pl_py, e_pl)
                raise

def parse_amount(expr: pl.Expr) -> pl.Expr:
    s = expr.cast(pl.Utf8, strict=False).str.strip_chars()
    if currs:
        s = s.str.replace_all("|".join(map(re.escape,currs)), "")
    if strip_th:
        s = s.str.replace_all(r"\.", "")
    s = s.str.replace_all(",", ".")
    return s.cast(pl.Float64, strict=False)

def tidy(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    for c in out.columns:
        if date_re.search(c):
            out = out.with_columns(
                pl.col(c).cast(pl.Utf8, strict=False)
                         .str.strip_chars()
                         .str.replace_all(r"[^0-9/:\-\s]", "")
                         .str.strptime(pl.Date, strict=False)
                         .alias(c)
            )
    for c in out.columns:
        if amt_re.search(c):
            out = out.with_columns(parse_amount(pl.col(c)).alias(c))
    return out

def process(p_in: Path, p_out: Path):
    logging.info(f"Cleaning: {p_in}")
    try:
        df = read_parquet_robust(p_in)
    except BaseException as e:
        logging.warning(f"SKIP (leitura falhou): {p_in} | {e}")
        return
    try:
        clean = tidy(df)
        p_out.parent.mkdir(parents=True, exist_ok=True)
        clean.write_parquet(p_out)
        logging.info(f"Clean → {p_out}")
    except BaseException as e:
        log_fail(p_in, e)
        logging.warning(f"SKIP (transform falhou): {p_in} | {e}")

def main():
    for p in Path(BRONZE).glob("**/*.parquet"):
        rel = p.relative_to(BRONZE)
        out = Path(SILVER)/rel
        process(p, out)

if __name__=="__main__":
    main()
