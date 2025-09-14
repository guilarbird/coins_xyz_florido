import openpyxl
from pathlib import Path
import glob
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    files = glob.glob("raw_data/01 Expenses Management*.xlsx")
    if not files:
        logging.warning("No '01 Expenses Management*.xlsx' file found in raw_data/")
        return
    workbook_path = max(files, key=lambda f: Path(f).stat().st_mtime)
    
    workbook = openpyxl.load_workbook(workbook_path)
    logging.info(workbook.sheetnames)

if __name__ == "__main__":
    main()
