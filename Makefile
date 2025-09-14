.PHONY: setup silver gold views peek agent all install dashboard

install:
	python -m pip install -q duckdb pandas pyarrow numpy pyyaml rapidfuzz openpyxl streamlit plotly

setup: install

silver:
	python scripts/extract_from_xlsx.py
	python scripts/build_silver_expenses.py
	python scripts/build_silver_revenue.py

gold:
	python scripts/build_gold_expenses.py
	python scripts/build_gold_revenue.py
	python scripts/load_duckdb.py

views:
	python scripts/load_duckdb.py

peek:
	sed -n '1,40p' gold/expenses_per_month_status.csv | column -s, -t || true; \
	sed -n '1,40p' gold/expenses_cumulative_2025.csv | column -s, -t || true; \
	sed -n '1,40p' gold/revenue_per_month.csv | column -s, -t || true; \
	sed -n '1,40p' gold/revenue_cumulative_2025.csv | column -s, -t || true

agent:
	python agent/finance_agent_cli.py
verify:
	python scripts/verify_results.py

dashboard:
	streamlit run app/app.py --server.port 7860 --server.address 0.0.0.0

all: install silver gold
