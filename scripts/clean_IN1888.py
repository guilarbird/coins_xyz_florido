import os
import pandas as pd

# === CONFIGURATION ===
ROOT_DIR = './IN1888'
CLEANED_DIR = './IN1888_cleaned'
OUTPUT_FILE = './IN1888_cleaned_merged.csv'
MAPPING_FILE = './business_line_mapping.csv'
SUMMARY_FILE = './IN1888_cleaning_summary.csv'

def infer_business_line(path):
    path_lower = path.lower()
    if 'otc' in path_lower:
        return 'OTC'
    elif 'exchange' in path_lower:
        return 'Exchange'
    elif 'payment' in path_lower:
        return 'Payments'
    else:
        return 'Unknown'

def extract_month(date_str):
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return pd.to_datetime(date_str, format=fmt).month
        except Exception:
            continue
    return None

def clean_file(filepath, business_line, file_summary):
    try:
        # Try Excel first
        try:
            df = pd.read_excel(filepath, engine='openpyxl')
        except Exception:
            df = pd.read_csv(filepath, encoding='latin1', sep=None, engine='python')
        orig_cols = df.columns.tolist()
        orig_rows = len(df)
        # Add business line and source file
        df['Business Line'] = business_line
        df['Source File'] = os.path.basename(filepath)
        # Try to extract month from first date column
        date_col = next((col for col in df.columns if 'data' in col.lower()), None)
        if date_col:
            df['Month'] = df[date_col].apply(lambda x: extract_month(str(x)))
        else:
            df['Month'] = None
        # Normalize CNPJ/CPF columns
        for col in df.columns:
            if 'cnpj' in col.lower() or 'cpf' in col.lower():
                df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(14)
        # Convert numerics
        for col in df.columns:
            if 'valor' in col.lower() or 'quantidade' in col.lower():
                df[col] = pd.to_numeric(df[col], errors='coerce')
        # Remove duplicates
        df = df.drop_duplicates()
        # Save individual cleaned file
        os.makedirs(CLEANED_DIR, exist_ok=True)
        cleaned_filename = os.path.splitext(os.path.basename(filepath))[0] + '_cleaned.csv'
        cleaned_path = os.path.join(CLEANED_DIR, cleaned_filename)
        df.to_csv(cleaned_path, index=False)
        # Log summary
        file_summary.append({
            'file': filepath,
            'rows_in': orig_rows,
            'rows_out': len(df),
            'columns_in': orig_cols,
            'columns_out': df.columns.tolist(),
            'business_line': business_line,
            'notes': f"Saved cleaned file to {cleaned_path}"
        })
        return df
    except Exception as e:
        file_summary.append({
            'file': filepath,
            'rows_in': None,
            'rows_out': None,
            'columns_in': None,
            'columns_out': None,
            'business_line': business_line,
            'notes': str(e)
        })
        return None

def main():
    all_data = []
    mapping = []
    file_summary = []
    for subdir, _, files in os.walk(ROOT_DIR):
        for file in files:
            filepath = os.path.join(subdir, file)
            business_line = infer_business_line(filepath)
            mapping.append({'file': file, 'suggested_business_line': business_line, 'path': filepath})
            df = clean_file(filepath, business_line, file_summary)
            if df is not None:
                all_data.append(df)
    if all_data:
        merged = pd.concat(all_data, ignore_index=True)
        merged.to_csv(OUTPUT_FILE, index=False)
        print(f"Saved merged data to {OUTPUT_FILE}")
    pd.DataFrame(mapping).to_csv(MAPPING_FILE, index=False)
    pd.DataFrame(file_summary).to_csv(SUMMARY_FILE, index=False)
    print(f"Saved mapping table to {MAPPING_FILE}")
    print(f"Saved cleaning summary to {SUMMARY_FILE}")

if __name__ == "__main__":
    main()