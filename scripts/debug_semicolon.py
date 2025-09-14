#!/usr/bin/env python3
import duckdb, pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB="warehouse/coins_xyz.duckdb"
VIEWS=[
    "silver__history_table_1__history_table_1",
    "silver__credit_card_history_table_1__credit_card_history_table_1",
    "silver__analysis_analysis_2__analysis_analysis_2",
    "silver__credit_card___analysis_credit_card___analysis_2__credit_card___analysis_credit_card___analysis_2",
]

def show(v):
    try:
        with duckdb.connect(DB) as con:
            df = con.execute(f"SELECT * FROM {v}").df()
    except Exception as e:
        logging.error(f"\n[{v}] erro:", exc_info=e); return
    logging.info(f"\n[{v}] shape={df.shape} cols={list(df.columns)}")
    # imprime 10 linhas por coluna, tokenizando por ';'
    for col in df.columns:
        logging.info(f"\n  >> coluna: {col}")
        for i, val in list(df[col].head(10).items()):
            s = "" if pd.isna(val) else str(val)
            toks = [p.strip() for p in s.split(";")]
            logging.info(f"    {i:>3}: {toks}")
    logging.info("-"*60)

for v in VIEWS:
    show(v)
