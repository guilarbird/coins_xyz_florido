#!/usr/bin/env python3
import duckdb, pandas as pd, numpy as np, hashlib
from pathlib import Path

DB="warehouse/coins_xyz.duckdb"
OUT=Path("gold"); OUT.mkdir(exist_ok=True)

con=duckdb.connect(DB)
df = con.execute("SELECT month, status, amount AS total FROM gold__expenses_canon").df()

# shape into a tx-like table
df["tx_date"] = df["month"]
df["amount_brl"] = df["total"]
for col in ["category","description","merchant","entity","bank","payment_type"]:
    df[col] = np.nan
df["source"] = "canon"
df["tx_id"] = [hashlib.sha1(f"{r.month}_{r.status}_{r.total}".encode()).hexdigest()[:16]
              for r in df.itertuples()]

cols = ["tx_id","tx_date","month","amount_brl","status","category","description",
        "merchant","entity","bank","payment_type","source"]
df = df[cols]
df.to_parquet(OUT/"expenses_tx.parquet", index=False)
print("Wrote gold/expenses_tx.parquet")
