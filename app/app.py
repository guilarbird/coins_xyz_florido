import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide")

@st.cache_data
def load_data():
    con = duckdb.connect("warehouse/coins_xyz.duckdb", read_only=True)
    expenses_df = con.execute("SELECT * FROM gold__expenses_per_month_status").df()
    expenses_cum_df = con.execute("SELECT * FROM gold__expenses_cumulative_2025").df()
    revenue_df = con.execute("SELECT * FROM gold__revenue_per_month").df()
    revenue_cum_df = con.execute("SELECT * FROM gold__revenue_cumulative_2025").df()
    con.close()
    return expenses_df, expenses_cum_df, revenue_df, revenue_cum_df

expenses_df, expenses_cum_df, revenue_df, revenue_cum_df = load_data()

st.title("Financial Dashboard")

# Expenses
st.header("Expenses")
fig_expenses_bars = px.bar(expenses_df, x="month", y="amount", color="status", title="Expenses per Month per Status (BRL)")
st.plotly_chart(fig_expenses_bars, use_container_width=True)

fig_expenses_cum = px.line(expenses_cum_df, x="month", y=["cum_completed", "cum_total"], title="Cumulative Expenses 2025 (BRL)")
st.plotly_chart(fig_expenses_cum, use_container_width=True)

# Revenue
st.header("Revenue")
fig_revenue_bars = px.bar(revenue_df, x="month", y="net_brl", title="Revenue per Month (BRL)")
st.plotly_chart(fig_revenue_bars, use_container_width=True)

fig_revenue_cum = px.line(revenue_cum_df, x="month", y="cum_total", title="Cumulative Revenue 2025 (BRL)")
st.plotly_chart(fig_revenue_cum, use_container_width=True)

# P&L
st.header("Profit & Loss")
pnl_df = pd.merge(expenses_df.groupby("month")["amount"].sum().reset_index(), revenue_df, on="month", how="outer").fillna(0)
pnl_df["pnl"] = pnl_df["net_brl"] - pnl_df["amount"]
pnl_df["cum_pnl"] = pnl_df["pnl"].cumsum()

fig_pnl_bars = px.bar(pnl_df, x="month", y="pnl", title="Monthly P&L (BRL)")
st.plotly_chart(fig_pnl_bars, use_container_width=True)

fig_pnl_cum = px.line(pnl_df, x="month", y="cum_pnl", title="Cumulative P&L 2025 (BRL)")
st.plotly_chart(fig_pnl_cum, use_container_width=True)
