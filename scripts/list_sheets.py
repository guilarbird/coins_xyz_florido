import openpyxl
from pathlib import Path
import glob

def main():
    files = glob.glob("raw_data/01 Expenses Management*.xlsx")
    if not files:
        print("No '01 Expenses Management*.xlsx' file found in raw_data/")
        return
    workbook_path = max(files, key=lambda f: Path(f).stat().st_mtime)
    
    workbook = openpyxl.load_workbook(workbook_path)
    print(workbook.sheetnames)

if __name__ == "__main__":
    main()
