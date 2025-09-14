#!/usr/bin/env python3
import duckdb, pandas as pd

DB="warehouse/coins_xyz.duckdb"
VIEWS=[
    "silver__history_table_1__history_table_1",
    "silver__credit_card_history_table_1__credit_card_history_table_1",
    "silver__analysis_analysis_2__analysis_analysis_2",
    "silver__credit_card___analysis_credit_card___analysis_2__credit_card___analysis_credit_card___analysis_2",
]

def show(v):
    try:
        df = duckdb.connect(DB).execute(f"SELECT * FROM {v}").df()
    except Exception as e:
        print(f"\n[{v}] erro:", e); return
    print(f"\n[{v}] shape={df.shape} cols={list(df.columns)}")
    # imprime 10 linhas por coluna, tokenizando por ';'
    for col in df.columns:
        print(f"\n  >> coluna: {col}")
        for i, val in list(df[col].head(10).items()):
            s = "" if pd.isna(val) else str(val)
            toks = [p.strip() for p in s.split(";")]
            print(f"    {i:>3}: {toks}")
    print("-"*60)

for v in VIEWS:
    show(v)
