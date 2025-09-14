#!/usr/bin/env python3
import duckdb
import re
import pandas as pd

con = duckdb.connect("warehouse/coins_xyz.duckdb", read_only=True)

HELP = """
Commands:
  bars expenses             → month/status totals for expenses
  cumulative expenses 2025  → cumulative expenses series for 2025
  bars revenue              → month/status totals for revenue
  cumulative revenue 2025   → cumulative revenue series for 2025
  pnl 2025                  → monthly and cumulative P&L for 2025
  bycat YYYY-MM             → category mix for a month
  find "text"               → search in descriptions
  unmatched                 → gold vs pivot diffs (needs gold/reconciliation_report.csv)
  help / ?                  → help
  quit / exit               → leave
"""

def show_bars_expenses():
    print(con.execute("SELECT * FROM gold__expenses_per_month_status ORDER BY month, status").df().to_string(index=False))

def show_cumulative_expenses(year=2025):
    print(con.execute(f"SELECT * FROM gold__expenses_cumulative_{year} ORDER BY month").df().to_string(index=False))

def show_bars_revenue():
    print(con.execute("SELECT * FROM gold__revenue_per_month ORDER BY month").df().to_string(index=False))

def show_cumulative_revenue(year=2025):
    print(con.execute(f"SELECT * FROM gold__revenue_cumulative_{year} ORDER BY month").df().to_string(index=False))

def show_pnl(year=2025):
    expenses_df = con.execute(f"SELECT month, SUM(amount) as expenses FROM gold__expenses_tx WHERE strftime(month, '%Y') = '{year}' GROUP BY month").df()
    revenue_df = con.execute(f"SELECT month, SUM(net_brl) as revenue FROM gold__revenue_tx WHERE strftime(month, '%Y') = '{year}' GROUP BY month").df()
    pnl_df = pd.merge(revenue_df, expenses_df, on="month", how="outer").fillna(0)
    pnl_df["pnl"] = pnl_df["revenue"] - pnl_df["expenses"]
    pnl_df["cum_pnl"] = pnl_df["pnl"].cumsum()
    print(pnl_df.to_string(index=False))

def show_bycat(ym):
    print(con.execute("""
      SELECT category, ROUND(SUM(amount),2) AS total
      FROM gold__expenses_tx
      WHERE strftime(month, '%Y-%m') = ?
      GROUP BY 1 ORDER BY 2 DESC
    """, [ym]).df().to_string(index=False))

def find_text(text):
    print(con.execute("""
      SELECT month::DATE AS month, tx_date, ROUND(amount,2) AS amount, category, description
      FROM gold__expenses_tx
      WHERE description ILIKE '%' || ? || '%'
      ORDER BY tx_date DESC
    """, [text]).df().head(100).to_string(index=False))

def unmatched():
    import pandas as pd
    try:
        df = pd.read_csv("gold/reconciliation_report.csv")
        df = df[df["diff"].abs() > 0.5].sort_values(["month", "status"])
        print("No mismatches > 0.5." if df.empty else df.to_string(index=False))
    except:
        print("No reconciliation_report.csv yet.")

def main():
    print("Finance Agent. Type 'help' for commands.")
    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break
        if not line:
            continue
        if line in ("quit", "exit"):
            break
        if line in ("help", "?"):
            print(HELP)
            continue
        if line == "bars expenses":
            show_bars_expenses()
            continue
        if line.startswith("cumulative expenses"):
            show_cumulative_expenses(2025)
            continue
        if line == "bars revenue":
            show_bars_revenue()
            continue
        if line.startswith("cumulative revenue"):
            show_cumulative_revenue(2025)
            continue
        if line.startswith("pnl"):
            show_pnl(2025)
            continue
        m = re.match(r"bycat\s+(\d{4}-\d{2})$", line)
        if m:
            show_bycat(m.group(1))
            continue
        m = re.match(r'find\s+"(.+)"$', line)
        if m:
            find_text(m.group(1))
            continue
        if line == "unmatched":
            unmatched()
            continue
        print("Unknown command. Type 'help'.")

if __name__ == "__main__":
    main()
