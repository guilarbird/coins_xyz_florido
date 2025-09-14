# ==============================================================================
# SCRIPT: discover_data_model.py
# PURPOSE: Scans the raw_data directory, profiles each CSV with robust error
#          handling, and generates a detailed data dictionary report.
# VERSION: 2.0 (With robust CSV parsing and error analysis)
# ==============================================================================

import pandas as pd
import os

def profile_csv_file(file_path):
    """
    Robustly reads a CSV file and returns a summary of its structure and content.
    Tries different separators and encodings.
    """
    try:
        # A list of common separators to try
        separators = [',', ';', '\t', '|']
        df = None
        detected_sep = None

        for sep in separators:
            try:
                # Try reading with the separator
                temp_df = pd.read_csv(file_path, sep=sep, low_memory=False)
                # A simple check: if we get more than one column, it's likely the correct separator
                if temp_df.shape[1] > 1:
                    df = temp_df
                    detected_sep = sep
                    break
            except Exception:
                continue # Try the next separator

        # If no separator worked, try one last time with Python's auto-detection
        if df is None:
            df = pd.read_csv(file_path, sep=None, engine='python', low_memory=False)
            detected_sep = "auto-detected"

        # Drop rows that are entirely empty
        df.dropna(how='all', inplace=True)
        df = df.reset_index(drop=True)

        num_rows, num_cols = df.shape
        columns = [str(col).strip() for col in df.columns.tolist()]
        sample_data = df.head(3).to_markdown(index=False)

        summary = {
            "status": "Success",
            "num_rows": num_rows,
            "num_cols": num_cols,
            "columns": columns,
            "detected_separator": detected_sep,
            "sample_data": sample_data
        }
        return summary
        
    except Exception as e:
        return {"status": "Failed", "error": str(e)}

if __name__ == "__main__":
    print("==============================================")
    print("Starting Data Model Discovery Script v2.0")
    print("==============================================")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    RAW_DATA_DIR = os.path.join(BASE_DIR, '..', 'raw_data')
    REPORT_FILE_PATH = os.path.join(BASE_DIR, '..', 'data_dictionary.md')

    report_content = "# Data Dictionary for Brazil Operations\n\n"
    report_content += "This report profiles all raw CSV files to help build a unified data model.\n\n"

    for filename in sorted(os.listdir(RAW_DATA_DIR)):
        if filename.endswith(".csv"):
            file_path = os.path.join(RAW_DATA_DIR, filename)
            
            if os.path.getsize(file_path) == 0:
                print(f"Skipping empty file: {filename}")
                continue

            print(f"Profiling file: {filename}...")
            profile = profile_csv_file(file_path)

            report_content += f"## File: `{filename}`\n\n"
            if profile["status"] == "Failed":
                report_content += f"- **Status:** <font color='red'>Failed</font>\n"
                report_content += f"- **Error:** {profile['error']}\n\n"
            else:
                report_content += f"- **Status:** <font color='green'>Success</font>\n"
                report_content += f"- **Detected Separator:** `{profile['detected_separator']}`\n"
                report_content += f"- **Rows:** {profile['num_rows']}\n"
                report_content += f"- **Columns:** {profile['num_cols']}\n"
                report_content += "- **Column Names:**\n"
                for col in profile['columns']:
                    report_content += f"  - `{col}`\n"
                report_content += "\n- **Data Sample:**\n"
                report_content += f"{profile['sample_data']}\n\n"
    
    with open(REPORT_FILE_PATH, 'w', encoding='utf-8') as f:
        f.write(report_content)

    print("\n==============================================")
    print(f"Data discovery complete. Report saved to: {REPORT_FILE_PATH}")
    print("==============================================")