#!/usr/bin/env python3
import duckdb, re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

HELP = """
Commands:
  bars                      → month/status totals
  cumulative [YYYY]         → cumulative series for a year (default: current year)
  bycat YYYY-MM             → category mix for a month
  find "text"               → search in descriptions
  unmatched                 → gold vs pivot diffs (needs gold/reconciliation_report.csv)
  help / ?                  → help
  quit / exit               → leave
"""
def show_bars(con):
    logging.info(con.execute("""
      SELECT month, status, ROUND(SUM(amount),2) AS total
      FROM gold__expenses_tx
      GROUP BY 1,2 ORDER BY 1,2
    """).df().to_string(index=False))
def show_cumulative(con, year):
    logging.info(con.execute(f"""
    WITH m AS (
      SELECT month,
             SUM(CASE WHEN status='Completed' THEN amount ELSE 0 END) AS completed,
             SUM(amount) AS total
      FROM gold__expenses_tx
      WHERE EXTRACT(YEAR FROM month)={year}
      GROUP BY 1
    )
    SELECT month,
           SUM(completed) OVER (ORDER BY month) AS cum_completed,
           SUM(total)     OVER (ORDER BY month) AS cum_total
    FROM m ORDER BY month
    """).df().to_string(index=False))
def show_bycat(con, ym):
    logging.info(con.execute("""
      SELECT category, ROUND(SUM(amount),2) AS total
      FROM gold__expenses_tx
      WHERE strftime(month, '%Y-%m') = ?
      GROUP BY 1 ORDER BY 2 DESC
    """,[ym]).df().to_string(index=False))
def find_text(con, text):
    logging.info(con.execute("""
      SELECT month::DATE AS month, tx_date, ROUND(amount,2) AS amount, category, description
      FROM gold__expenses_tx
      WHERE description ILIKE '%' || ? || '%'
      ORDER BY tx_date DESC
    """,[text]).df().head(100).to_string(index=False))
def unmatched():
    import pandas as pd
    try:
        df = pd.read_csv("gold/reconciliation_report.csv")
        df = df[df["diff"].abs()>0.5].sort_values(["month","status"])
        logging.info("No mismatches > 0.5." if df.empty else df.to_string(index=False))
    except: logging.warning("No reconciliation_report.csv yet.")
def main():
    logging.info("Expense Agent (gold__* views). Type 'help' for commands.")
    with duckdb.connect("warehouse/coins_xyz.duckdb") as con:
        while True:
            try: line = input("> ").strip()
            except EOFError: break
            if not line: continue
            if line in ("quit","exit"): break
            if line in ("help","?"): logging.info(HELP); continue
            if line=="bars": show_bars(con); continue
            m=re.match(r"cumulative(\s+(\d{4}))?$", line)
            if m:
                year = int(m.group(2)) if m.group(2) else datetime.now().year
                show_cumulative(con, year); continue
            m=re.match(r"bycat\s+(\d{4}-\d{2})$", line)
            if m: show_bycat(con, m.group(1)); continue
            m=re.match(r'find\s+"(.+)"$', line)
            if m: find_text(con, m.group(1)); continue
            if line=="unmatched": unmatched(); continue
            logging.warning("Unknown command. Type 'help'.")
from datetime import datetime
if __name__ == "__main__":
    main()
