# ==============================================================================
# SCRIPT: process_financial_data.py
# PURPOSE: Cleans and structures raw markdown financial reports from Coins.xyz Brazil
#          into machine-readable CSV files suitable for a vector store and data analysis.
# AUTHOR: Your AI Assistant
# LAST UPDATED: August 25, 2025 (with fix for expense date parsing format)
# ==============================================================================

import pandas as pd
import re
import os

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def clean_currency(value):
    """
    Universal function to clean currency strings.
    Handles formats like 'R$ 5.000,00', '$886,52', or just numbers.
    """
    if isinstance(value, str):
        value = value.replace('R$', '').replace('$', '').replace('%', '').strip()
        value = value.replace('.', '').replace(',', '.')
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    return value

# ==============================================================================
# PROCESSING FUNCTIONS FOR EACH FILE
# ==============================================================================

def process_balance_snapshot(file_path):
    """Cleans the 'Balances SnapShot' markdown file."""
    print(f"--- Starting: {file_path} ---")
    try:
        df = pd.read_csv(file_path, sep='|', skipinitialspace=True, header=1)
        df.columns = [col.strip() for col in df.columns]
        df = df.drop(columns=['Unnamed: 0', df.columns[-1]], errors='ignore')
        
        df = df.rename(columns={df.columns[1]: 'datetime'})
        df['datetime'] = pd.to_datetime(df['datetime'], format='%d/%m/%Y %H:%M', errors='coerce')
        
        currency_cols = ['absolute volume', 'volume amount', 'Account Volume (USD)', 'Account Volume (BRL)', 'ptax']
        for col in currency_cols:
            if col in df.columns:
                df[col] = df[col].apply(clean_currency)

        df.dropna(how='all', inplace=True)
        print(f"SUCCESS: Processed {len(df)} valid rows from {file_path}.")
        return df
    except Exception as e:
        print(f"ERROR processing {file_path}: {e}")
        return None

def process_expenses(file_path):
    """Cleans the 'Expenses Management' markdown file."""
    print(f"--- Starting: {file_path} ---")
    try:
        df = pd.read_csv(file_path, sep='|', skipinitialspace=True, header=1)
        df.columns = [col.strip() for col in df.columns]
        df = df.drop(columns=['Unnamed: 0', df.columns[-1]], errors='ignore')

        df = df.iloc[:, :11]
        
        clean_columns = [
            'Date', 'Expense Description', 'Category', 'Tax Name', 'Freq',
            'Amount', 'Base de Calculo', 'Aliquota', 'Faturamento Estimado',
            'Entity', 'Obs'
        ]
        df.columns = clean_columns

        # FIX APPLIED HERE: Changed date format from day-first ('%d-%m-%Y') to month-first ('%m-%d-%Y').
        df['Date'] = pd.to_datetime(df['Date'], format='%m-%d-%Y', errors='coerce')
        
        df['Amount'] = df['Amount'].apply(clean_currency)
        df.dropna(how='all', inplace=True)
        print(f"SUCCESS: Processed {len(df)} valid rows from {file_path}.")
        return df
    except Exception as e:
        print(f"ERROR processing {file_path}: {e}")
        return None

def process_referral_reports(file_path):
    """Cleans the 'Referral Reports' markdown file."""
    print(f"--- Starting: {file_path} ---")
    try:
        df = pd.read_csv(file_path, sep='|', skipinitialspace=True, header=1)
        df.columns = [col.strip() for col in df.columns]
        df = df.drop(columns=['Unnamed: 0', df.columns[-1]], errors='ignore')
        df.dropna(subset=[df.columns[0]], inplace=True)

        for col in df.columns:
            if df[col].astype(str).str.contains('[R$|%]', regex=True).any():
                df[col] = df[col].apply(clean_currency)

        print(f"SUCCESS: Processed {len(df)} valid rows from {file_path}.")
        return df
    except Exception as e:
        print(f"ERROR processing {file_path}: {e}")
        return None

def process_sell_usdt_trades(file_path):
    """Cleans the 'Sell USDT Trades' markdown file."""
    print(f"--- Starting: {file_path} ---")
    try:
        df = pd.read_csv(file_path, sep='|', skipinitialspace=True, header=3)
        df.columns = [col.strip() for col in df.columns]
        df = df.drop(columns=['Unnamed: 0', df.columns[-1]], errors='ignore')
        df.dropna(subset=['Trade ID'], inplace=True)
        
        df = df.rename(columns={df.columns[1]: 'Date'})
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')
        
        numeric_cols = ['Volume', 'Fee', 'Volume BRL', 'LP Trade Fee (bps)', 'LP Trade Fee (R$)', 'Total (BRL)', 'Total (USD)']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).apply(clean_currency), errors='coerce')

        print(f"SUCCESS: Processed {len(df)} valid rows from {file_path}.")
        return df
    except Exception as e:
        print(f"ERROR processing {file_path}: {e}")
        return None

def process_pricer_data(file_path):
    """Extracts key-value pairs from the 'Pricer' markdown file."""
    print(f"--- Starting: {file_path} ---")
    try:
        data = {}
        with open(file_path, 'r') as f:
            for line in f:
                parts = [p.strip() for p in line.split('|') if p.strip()]
                if len(parts) >= 2:
                    key = parts[0]
                    value = parts[1]
                    if key and value and key != 'nan' and value != 'nan':
                        data[key] = value
        
        df = pd.DataFrame(list(data.items()), columns=['Metric', 'Value'])
        print(f"SUCCESS: Extracted {len(df)} key-value pairs from {file_path}.")
        return df
    except Exception as e:
        print(f"ERROR processing {file_path}: {e}")
        return None

# ==============================================================================
# MAIN EXECUTION BLOCK
# ==============================================================================

if __name__ == "__main__":
    print("==============================================")
    print("Starting Coins.xyz Brazil Data Cleaning Script")
    print("==============================================")

    # Define the input and output directories relative to the script's location
    RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'raw_data')
    CLEAN_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'clean_data')

    if not os.path.exists(CLEAN_DATA_DIR):
        os.makedirs(CLEAN_DATA_DIR)

    files_to_process = {
        '00 Balances SnapShot - tbBalancesSnapshots (1).md': (process_balance_snapshot, 'cleaned_balance_snapshot.csv'),
        '01 Expenses Management - tax analysis.md': (process_expenses, 'cleaned_expenses.csv'),
        '03 Trade History - Coins BR - Referral Reports.md': (process_referral_reports, 'cleaned_referral_reports.csv'),
        '03 Sell USDT Trades - LP.md': (process_sell_usdt_trades, 'cleaned_sell_usdt_trades.csv'),
        '03 Pricer - BR - Pricer.md': (process_pricer_data, 'cleaned_pricer_data.csv')
    }

    for raw_filename, (processing_func, clean_filename) in files_to_process.items():
        raw_file_path = os.path.join(RAW_DATA_DIR, raw_filename)
        clean_file_path = os.path.join(CLEAN_DATA_DIR, clean_filename)
        
        try:
            clean_df = processing_func(raw_file_path)
            if clean_df is not None:
                clean_df.to_csv(clean_file_path, index=False)
                print(f"--> Saved clean data to '{clean_file_path}'\n")
        except FileNotFoundError:
            print(f"SKIPPED: The file '{raw_file_path}' was not found.\n")
        except Exception as e:
            print(f"--> FAILED to process '{raw_filename}' due to an unexpected error: {e}\n")

    print("==============================================")
    print("Data cleaning process finished.")
    print("==============================================")