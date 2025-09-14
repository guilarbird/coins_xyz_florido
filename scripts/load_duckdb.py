#!/usr/bin/env python3
import duckdb
from pathlib import Path
DB="warehouse/coins_xyz.duckdb"
con = duckdb.connect(DB)
for root in ("silver","gold"):
  for fp in Path(root).glob("**/*.parquet"):
    rel = fp.relative_to(root).with_suffix("")
    parts = [root] + list(rel.parts)
    tbl = "__".join(parts).replace("-","_")
    con.execute(f"CREATE OR REPLACE VIEW {tbl} AS SELECT * FROM '{fp.as_posix()}';")
    print("VIEW:", tbl)
print("Total views:", len(con.execute("SHOW TABLES").fetchall()))
