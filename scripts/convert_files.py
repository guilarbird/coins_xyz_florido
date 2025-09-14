import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def convert_csv_to_markdown(downloads_path):
    """
    Finds specific CSV files in the user's Downloads folder
    and converts them to Markdown (.md) files.
    """
    # List of the exact filenames you want to convert
    files_to_convert = [
        '00 Balances SnapShot - tbBalancesSnapshots (1).csv',
        '01 Expenses Management - tax analysis.csv',
        '03 Trade History - Coins BR - Referral Reports.csv',
        'Coins.xyz Global Operations  - System Access (1).csv'
    ]

    logging.info("Starting conversion process...\n")

    # Loop through the list of filenames
    for filename in files_to_convert:
        # Create the full path to the source CSV file
        source_file_path = os.path.join(downloads_path, filename)

        # Create the new filename for the output Markdown file
        # This replaces '.csv' with '.md'
        output_filename = filename.replace('.csv', '.md')
        output_file_path = os.path.join(downloads_path, output_filename)

        try:
            # Check if the source file actually exists
            if not os.path.exists(source_file_path):
                logging.warning(f"SKIPPING: Cannot find the file '{filename}' in Downloads.")
                continue # Move to the next file in the list

            # Read the CSV file into a pandas DataFrame
            # Added error_bad_lines=False and warn_bad_lines=True for robustness
            logging.info(f"Reading '{filename}'...")
            df = pd.read_csv(source_file_path, on_bad_lines='warn')

            # Convert the DataFrame to a Markdown formatted string
            # index=False prevents writing row numbers into the file
            markdown_content = df.to_markdown(index=False)

            # Write the new Markdown content to the output file
            with open(output_file_path, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            logging.info(f"SUCCESS: Converted '{filename}' to '{output_filename}'\n")

        except Exception as e:
            # Catch any other errors during processing
            logging.error(f"ERROR: Failed to convert '{filename}'. Reason: {e}\n")

# --- Main part of the script ---
if __name__ == "__main__":
    # Get the current user's home directory and build the path to Downloads
    home_directory = os.path.expanduser('~')
    downloads_folder = os.path.join(home_directory, 'Downloads')
    
    # Run the conversion function
    convert_csv_to_markdown(downloads_folder)

    logging.info("...Conversion complete.")
