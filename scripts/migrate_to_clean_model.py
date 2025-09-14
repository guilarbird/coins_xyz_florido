import pandas as pd
import os
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# Function to authenticate with Google Sheets
def authenticate_gspread():
    # Replace with your credentials file path
    creds_path = os.path.join(os.path.dirname(__file__), '..', 'credentials.json')
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(creds)
    return client

# Function to open a Google Sheet and worksheet
def open_worksheet(client, sheet_name, worksheet_name):
    sheet = client.open(sheet_name)
    worksheet = sheet.worksheet(worksheet_name)
    return worksheet

if __name__ == "__main__":
    # Authenticate and open the worksheet
    gspread_client = authenticate_gspread()
    
    # Define the new data models
    balance_snapshots_df = pd.DataFrame(columns=['snapshot_date', 'platform', 'account_id', 'description', 'entity', 'asset_currency', 'balance_native', 'balance_usd', 'nature'])
    expenses_df = pd.DataFrame(columns=['expense_date', 'description', 'category', 'entity', 'amount_brl', 'tax_type'])
    trade_history_df = pd.DataFrame(columns=['trade_date', 'client_name', 'trade_type', 'volume_brl', 'client_rate', 'cost_rate', 'gross_profit_brl', 'referral_partner', 'commission_brl', 'net_profit_brl'])
    system_access_df = pd.DataFrame(columns=['employee_name', 'system_name', 'access_status', 'cost_per_month_usd', 'notes'])

    # Get the worksheet
    balance_snapshots_worksheet = open_worksheet(gspread_client, "Coins Brazil - Master Data Model", "balance_snapshots")
    expenses_worksheet = open_worksheet(gspread_client, "Coins Brazil - Master Data Model", "expenses")
    trade_history_worksheet = open_worksheet(gspread_client, "Coins Brazil - Master Data Model", "trade_history")
    system_access_worksheet = open_worksheet(gspread_client, "Coins Brazil - Master Data Model", "system_access")

    # Clear the worksheets before writing new data
    balance_snapshots_worksheet.clear()
    expenses_worksheet.clear()
    trade_history_worksheet.clear()
    system_access_worksheet.clear()

    # Write the dataframes to the worksheets
    set_with_dataframe(balance_snapshots_worksheet, balance_snapshots_df)
    set_with_dataframe(expenses_worksheet, expenses_df)
    set_with_dataframe(trade_history_worksheet, trade_history_df)
    set_with_dataframe(system_access_worksheet, system_access_df)

    print("Successfully created the Google Sheets workbook with the new data model.")
