#!/usr/bin/env python3
import re
from pathlib import Path
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq

RAW="raw_data"; BRONZE="bronze"

def slug(s:str)->str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]+","",s, flags=re.UNICODE)
    s = re.sub(r"\s+","_",s)
    return s

def read_csv_robust(path: Path) -> pd.DataFrame:
    # tentativas com encodings e separadores diferentes
    encs = ["utf-8-sig","latin-1","cp1252"]
    seps = [None, ";", ",", "\t", "|"]  # None = sniff
    # 1) tentativa rápida padrão
    try:
        return pd.read_csv(path, dtype_backend="pyarrow", encoding="utf-8-sig")
    except Exception:
        pass
    # 2) brute-force leve
    for enc in encs:
        for sep in seps:
            try:
                return pd.read_csv(
                    path,
                    dtype_backend="pyarrow",
                    engine="python",
                    sep=sep,
                    encoding=enc,
                    on_bad_lines="skip",
                )
            except Exception:
                continue
    # 3) último recurso: 1 coluna "raw"
    return pd.read_csv(
        path, engine="python", encoding="utf-8-sig", on_bad_lines="skip",
        header=None, names=["raw"]
    )

def to_parquet(df: pd.DataFrame, out: Path):
    out.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out, compression="zstd")
    (out.with_suffix(".schema.txt")).write_text(str(table.schema))

def ingest_file(path: Path):
    if path.suffix.lower()==".csv":
        df = read_csv_robust(path)
        df.columns = [slug(c) for c in df.columns]
        out = Path(BRONZE)/slug(path.stem)/(slug(path.stem)+".parquet")
        to_parquet(df, out); print("CSV →", out)
    elif path.suffix.lower() in (".xlsx",".xls"):
        xls = pd.ExcelFile(path)
        for sheet in xls.sheet_names:
            df = pd.read_excel(path, sheet_name=sheet, dtype_backend="pyarrow", engine="openpyxl")
            df.columns = [slug(c) for c in df.columns]
            out = Path(BRONZE)/slug(path.stem)/f"{slug(sheet)}.parquet"
            to_parquet(df, out); print(f"XLSX({sheet}) →", out)
    else:
        print("Ignorando:", path.name)

def main():
    for p in Path(RAW).glob("*"):
        ingest_file(p)

if __name__=="__main__":
    main()
