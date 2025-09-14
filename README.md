# Coins Brazil Financial Data Consolidation

This project consolidates financial and operational data for the Coins Brazil team into a single, clean, and reliable master dataset.

## How to Run the Script

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
2. **Run the processing script:**
   ```bash
   python scripts/process_financial_data.py
   ```

## Maintaining Categorization Rules

The transaction categorization is managed by a set of rules defined in `categorization_rules.yml`. To modify the categorization logic, simply update this file. The script will automatically apply the new rules the next time it's run.
