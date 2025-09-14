# ==============================================================================
# SCRIPT: process_financial_data.py
# PURPOSE: Universal script to automatically find, clean, and structure a variety
#          of raw data files from the 'raw_data' directory.
# VERSION: 4.0 (Handles multiple file types, messy headers, and categorization)
# ==============================================================================

import pandas as pd
import os
import yaml

def find_header_and_load(file_path, header_keyword):
    """Finds the real header row in a messy CSV and reloads the file from there."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if header_keyword in line:
                    # Reload the CSV, skipping rows until the header
                    return pd.read_csv(file_path, header=i, sep=',', low_memory=False, encoding='utf-8')
    except UnicodeDecodeError:
         with open(file_path, 'r', encoding='latin1') as f:
            for i, line in enumerate(f):
                if header_keyword in line:
                    return pd.read_csv(file_path, header=i, sep=',', low_memory=False, encoding='latin1')
    return None # Return None if keyword not found

def normalize_columns(df):
    """Standardizes column names to a common format."""
    # Add your column normalization logic here
    return df

def categorize_transaction(description, rules):
    """Categorizes a transaction based on keywords in its description."""
    for rule in rules['rules']:
        for keyword in rule['keywords']:
            if keyword in description.lower():
                return rule['category']
    return 'Uncategorized'

# ==============================================================================
# SPECIALIZED CLEANING FUNCTIONS
# ==============================================================================

def process_trade_history_bi(df):
    """Cleans the main, well-structured Trade History BI file."""
    print("  - Applying 'Trade History BI' cleaning recipe...")
    # This file is already well-structured, so we just return it.
    return df

def process_pnl_report(df):
    """Cleans a PNL & Volume Report."""
    print("  - Applying 'PNL Report' cleaning recipe...")
    df.dropna(axis=1, how='all', inplace=True)
    df.columns.name = None
    df = df.rename(columns=lambda c: str(c).strip())
    # Select only the main weekly data table
    weekly_data = df.iloc[:, 11:19]
    weekly_data.columns = [
        'Week', 'FX_Volume', 'OTC_Volume', 'Total_Volume', 
        'FX_Income', 'OTC_Income', 'Total_Income', 'BPS'
    ]
    weekly_data = weekly_data.dropna(subset=['Week'])
    return weekly_data

def process_generic_trade_log(df):
    """Generic cleaning for other trade logs like LP Trades and Sell USDT."""
    print("  - Applying 'Generic Trade Log' cleaning recipe...")
    df.dropna(axis=1, how='all', inplace=True)
    df.columns.name = None
    df = df.rename(columns=lambda c: str(c).strip())
    # Drop rows that are likely empty or comments
    df = df.dropna(subset=[df.columns[0]])
    return df

# ==============================================================================
# MAIN EXECUTION BLOCK
# ==============================================================================

if __name__ == "__main__":
    print("==============================================")
    print("Starting Universal Data Cleaning Script v4.0")
    print("==============================================")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    RAW_DATA_DIR = os.path.join(BASE_DIR, '..', 'raw_data')
    CLEAN_DATA_DIR = os.path.join(BASE_DIR, '..', 'clean_data')
    CONFIG_PATH = os.path.join(BASE_DIR, '..', 'categorization_rules.yml')

    if not os.path.exists(CLEAN_DATA_DIR):
        os.makedirs(CLEAN_DATA_DIR)

    # Load categorization rules
    with open(CONFIG_PATH, 'r') as f:
        rules = yaml.safe_load(f)

    all_dataframes = []

    # Automatically scan the raw_data directory
    for raw_filename in sorted(os.listdir(RAW_DATA_DIR)):
        raw_file_path = os.path.join(RAW_DATA_DIR, raw_filename)
        if os.path.isdir(raw_file_path) or not raw_filename.endswith(".csv") or os.path.getsize(raw_file_path) == 0:
            continue

        print(f"\nProcessing file: {raw_filename}")
        
        try:
            filename_lower = raw_filename.lower()
            df_clean = None

            # --- FILE ROUTER ---
            if "trade history bi" in filename_lower:
                df_raw = pd.read_csv(raw_file_path)
                df_clean = process_trade_history_bi(df_raw)
            elif "pnl & volume report" in filename_lower:
                df_raw = find_header_and_load(raw_file_path, "TradeDesk Summary")
                if df_raw is not None:
                    df_clean = process_pnl_report(df_raw)
            elif "lp trades" in filename_lower or "sell usdt" in filename_lower:
                df_raw = find_header_and_load(raw_file_path, "Trade ID")
                if df_raw is not None:
                    df_clean = process_generic_trade_log(df_raw)
            elif "pricer" in filename_lower:
                # For now, we treat pricer files as raw data, just copying them
                df_clean = pd.read_csv(raw_file_path)
                print("  - INFO: Copying 'Pricer' file as-is.")
            else:
                print(f"  - WARNING: No cleaning rule for '{raw_filename}'. Skipping.")
                continue

            if df_clean is not None and not df_clean.empty:
                df_clean = normalize_columns(df_clean)
                df_clean['source_file'] = raw_filename
                df_clean['business_line'] = 'Unknown' # Placeholder
                # Add categorization logic here if applicable
                all_dataframes.append(df_clean)
                print(f"  --> Successfully processed and added to master dataset.")
            else:
                 print(f"  - INFO: No data processed or file was empty for '{raw_filename}'.")

        except Exception as e:
            print(f"  --> FAILED to process '{raw_filename}': {e}")

    # Create the unified master dataset
    if all_dataframes:
        master_df = pd.concat(all_dataframes, ignore_index=True)
        master_file_path = os.path.join(CLEAN_DATA_DIR, 'master_financial_dataset.csv')
        master_df.to_csv(master_file_path, index=False)
        print(f"\n--> Master dataset created at '{master_file_path}'")
    else:
        print("\n--> No dataframes to concatenate. Master dataset not created.")


    print("\n==============================================")
    print("Data cleaning process finished.")
    print("==============================================")
