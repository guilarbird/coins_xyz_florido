#!/usr/bin/env python3
import duckdb, re, os
from pathlib import Path
from datetime import datetime
import argparse

DB="warehouse/coins_xyz.duckdb"
OUT="gold"
Path(OUT).mkdir(exist_ok=True)

def get_year():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Reporting year")
    args = parser.parse_args()
    if args.year:
        return args.year
    if os.getenv("REPORTING_YEAR"):
        return int(os.getenv("REPORTING_YEAR"))
    return datetime.now().year

with duckdb.connect(DB) as con:
    year = get_year()
    # Optional overrides if your forecast lives in a specific view/columns
    F_VIEW  = os.getenv("FORECAST_VIEW")           # e.g. silver__analysis_table_1__analysis_table_1
    F_MONTH = os.getenv("FORECAST_MONTH_COL")      # e.g. month
    F_PROJ  = os.getenv("FORECAST_PROJECTED_COL")  # e.g. projected

# BRL parser: drop non-numerics, remove thousands ".", swap ","->".", cast to DOUBLE
def PARSE_BRL(col):
    return f"""CAST(
      REPLACE(
        REPLACE(
          REGEXP_REPLACE(CAST({col} AS VARCHAR), '[^0-9,.-]', '', 'g'),
          '.', ''
        ),
        ',', '.'
      ) AS DOUBLE
    )"""

def pick(cols,*opts):
    cols=[c.lower() for c in cols]
    for o in opts:
        if o in cols: return o
    for c in cols:
        for o in opts:
            if re.search(o, c, re.I): return c
    return None

    # --- pick best "actuals" table (History-like) ---
    tabs = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    def best_actuals():
        best,score=None,-1
        for t in tabs:
            cols=[r[0].lower() for r in con.execute(f"DESCRIBE {t}").fetchall()]
            s = 0
            s += any(re.search(r"(?:^|_)(date|data|dt)(?:_|$)", c, re.I) for c in cols)
            s += any(re.search(r"(?:^|_)(amount|valor|value)(?:_|$)", c, re.I) for c in cols)
            if "history" in t: s+=1
            if "expenses" in t: s+=.5
            if s>score: best,score=t,s
        return best, [r[0].lower() for r in con.execute(f"DESCRIBE {best}").fetchall()]

    a_tbl, a_cols = best_actuals()
    a_date = pick(a_cols,"date","data","dt","transaction_date")
    a_amt  = pick(a_cols,"amount","valor","value")
    a_cat  = pick(a_cols,"category","categoria") or "NULL"
    a_desc = pick(a_cols,"description","descricao","details","memo") or "NULL"

    if not (a_date and a_amt):
        raise SystemExit("Actuals missing date/amount columns.")

    print(f"[expenses] actuals source = {a_tbl}  (date={a_date} amount={a_amt})")

    # 1) Completed rows → gold/expenses_canon.parquet
    con.execute(f"""
    COPY (
      SELECT
        CAST({a_date} AS DATE)                      AS tx_date,
        date_trunc('month', CAST({a_date} AS DATE)) AS month,
        {PARSE_BRL(a_amt)}                          AS amount,
        'Completed'                                  AS status,
        {a_cat}                                      AS category,
        {a_desc}                                     AS description,
        '{a_tbl}'                                    AS source
      FROM {a_tbl}
      WHERE {a_date} IS NOT NULL
    ) TO '{OUT}/expenses_canon.parquet' (FORMAT PARQUET);
    """)
    print("[expenses] wrote Completed rows to gold/expenses_canon.parquet")

    # 2) Projected (monthly), if overrides are set
    if F_VIEW:
        cols=[r[0].lower() for r in con.execute(f"DESCRIBE {F_VIEW}").fetchall()]
        f_month = F_MONTH or pick(cols,"month","year_month","date","data")
        f_proj  = F_PROJ  or pick(cols,"projected","forecast")
        if not (f_month and f_proj):
            raise SystemExit("Set FORECAST_MONTH_COL and FORECAST_PROJECTED_COL for the chosen FORECAST_VIEW.")
        print(f"[expenses] forecast source = {F_VIEW}  (month={f_month} projected={f_proj})")
        con.execute(f"""
        COPY (
          SELECT
            CAST(date_trunc('month', {f_month}) AS DATE) AS tx_date,
            CAST(date_trunc('month', {f_month}) AS DATE) AS month,
            {PARSE_BRL(f_proj)}                         AS amount,
            'Projected'                                  AS status,
            NULL::VARCHAR                                AS category,
            NULL::VARCHAR                                AS description,
            '{F_VIEW}'                                   AS source
          FROM {F_VIEW}
          WHERE {f_proj} IS NOT NULL
        ) TO '{OUT}/expenses_canon.parquet' (FORMAT PARQUET, APPEND TRUE);
        """)
        print("[expenses] appended Projected rows to gold/expenses_canon.parquet")
    else:
        print("[expenses] no FORECAST_VIEW set → only Completed will be produced")

    # 3) Per-month per-status bars (this powers your blue/red pillars)
    con.execute(f"""
    COPY (
      SELECT month, status, SUM(amount) AS total
      FROM read_parquet('{OUT}/expenses_canon.parquet')
      GROUP BY 1,2 ORDER BY 1,2
    ) TO '{OUT}/expenses_per_month_status.parquet' (FORMAT PARQUET);
    """)
    print("[expenses] wrote gold/expenses_per_month_status.parquet")

    # 4) Cumulative year (two series: cum_completed and cum_total)
    con.execute(f"""
    COPY (
      WITH m AS (
        SELECT date_trunc('month', tx_date) AS month,
               SUM(CASE WHEN status='Completed' THEN amount ELSE 0 END) AS completed,
               SUM(amount) AS total
        FROM read_parquet('{OUT}/expenses_canon.parquet')
        WHERE EXTRACT(YEAR FROM tx_date)={year}
        GROUP BY 1
      )
      SELECT month,
             SUM(completed) OVER (ORDER BY month) AS cum_completed,
             SUM(total)     OVER (ORDER BY month) AS cum_total
      FROM m ORDER BY month
    ) TO '{OUT}/expenses_cumulative_{year}.parquet' (FORMAT PARQUET);
    """)
    print(f"[expenses] wrote gold/expenses_cumulative_{year}.parquet")
    print("[done] Gold rebuilt successfully.")
