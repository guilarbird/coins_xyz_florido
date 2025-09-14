#!/usr/bin/env python3
import duckdb, re
con = duckdb.connect("warehouse/coins_xyz.duckdb")
tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]

date_rx   = re.compile(r"(?:^|_)(date|data|dt)(?:_|$)", re.I)
amount_rx = re.compile(r"(?:^|_)(amount|valor|value)(?:_|$)", re.I)
cat_rx    = re.compile(r"(?:^|_)(category|categoria)(?:_|$)", re.I)
forecast_cols = re.compile(r"(projected|forecast)", re.I)

cands = []
for t in tables:
  cols = [r[0].lower() for r in con.execute(f"DESCRIBE {t}").fetchall()]
  score = 0
  score += any(date_rx.search(c) for c in cols)
  score += any(amount_rx.search(c) for c in cols)
  if "history" in t: score += 0.5
  if "expenses" in t: score += 0.5
  if any(forecast_cols.search(c) for c in cols): score += 1.0  # provável tabela base do Visuals
  cands.append((score, t, cols))
cands.sort(reverse=True, key=lambda x: x[0])
for s,t,cols in cands[:12]:
  print(f"{s:>4} | {t} | {', '.join(cols[:10])}{' ...' if len(cols)>10 else ''}")
