import os
import glob
import pandas as pd
import logging
from collections import Counter

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define directories relative to the script location
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PROCESSED_CSV_DIR = os.path.join(PROJECT_ROOT, "data/processed_csv_files")

def analyze_offenses():
    """Loads processed CSVs and analyzes the frequency of offense types."""
    logging.info(f"Looking for processed CSV files in: {PROCESSED_CSV_DIR}")
    csv_files = glob.glob(os.path.join(PROCESSED_CSV_DIR, '*_geocoded.csv'))

    if not csv_files:
        logging.error(f"No *_geocoded.csv files found in {PROCESSED_CSV_DIR}. Cannot analyze offense types.")
        return

    logging.info(f"Found {len(csv_files)} geocoded CSV files to analyze.")

    all_offenses = []
    for f in csv_files:
        try:
            df = pd.read_csv(f)
            if 'offense_type' in df.columns:
                # Drop NaN values before extending
                all_offenses.extend(df['offense_type'].dropna().tolist())
            else:
                logging.warning(f"Skipping file {os.path.basename(f)} as it lacks 'offense_type' column.")
        except pd.errors.EmptyDataError:
            logging.warning(f"Skipping empty file: {os.path.basename(f)}")
        except Exception as e:
            logging.error(f"Error reading file {os.path.basename(f)}: {e}")

    if not all_offenses:
        logging.error("No offense_type data found in any files.")
        return

    # Calculate frequencies
    offense_counts = Counter(all_offenses)
    total_offenses = len(all_offenses)
    unique_offense_count = len(offense_counts)

    logging.info(f"Total offense entries analyzed: {total_offenses}")
    logging.info(f"Number of unique offense types: {unique_offense_count}")
    print("\\n--- Offense Type Frequencies (Sorted) ---")

    # Sort by frequency descending
    sorted_offenses = sorted(offense_counts.items(), key=lambda item: item[1], reverse=True)

    # Print results
    for offense, count in sorted_offenses:
        print(f"{offense}: {count}")

    print("\\n----------------------------------------")

if __name__ == "__main__":
    analyze_offenses() 