import os
import glob
import pandas as pd
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define directories - Adjust paths relative to the script location (scripts/)
# Or use absolute paths based on project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PROCESSED_CSV_DIR = os.path.join(PROJECT_ROOT, "data/processed_csv_files")
WEBSITE_DATA_DIR = os.path.join(PROJECT_ROOT, "website/public/data")
OUTPUT_JSON_PATH = os.path.join(WEBSITE_DATA_DIR, "incidents.json")

# Columns to select for the frontend
# Keeping necessary info for mapping and potential filtering/tooltips
RELEVANT_COLUMNS = [
    'case_number',
    'date',
    'time',
    'offense_type',
    'location', # Original location string
    'latitude',
    'longitude',
    'formatted_address',
    'google_maps_uri',
    'place_types',
    'location_interpretation'
]

def prepare_data_for_website():
    """Loads processed CSVs, combines them, filters, and saves as JSON for the website."""
    logging.info(f"Looking for processed CSV files in: {PROCESSED_CSV_DIR}")
    csv_files = glob.glob(os.path.join(PROCESSED_CSV_DIR, '*_geocoded.csv'))

    if not csv_files:
        logging.error(f"No *_geocoded.csv files found in {PROCESSED_CSV_DIR}. Cannot prepare website data.")
        return

    logging.info(f"Found {len(csv_files)} geocoded CSV files to combine.")

    all_data_frames = []
    for f in csv_files:
        try:
            df = pd.read_csv(f)
            # Add a column to track the source file if needed (optional)
            # df['source_file'] = os.path.basename(f)
            all_data_frames.append(df)
        except pd.errors.EmptyDataError:
            logging.warning(f"Skipping empty file: {f}")
        except Exception as e:
            logging.error(f"Error reading file {f}: {e}")

    if not all_data_frames:
        logging.error("No data loaded from CSV files. Cannot proceed.")
        return

    # Combine all dataframes
    combined_df = pd.concat(all_data_frames, ignore_index=True)
    logging.info(f"Combined data contains {len(combined_df)} total records.")

    # Select relevant columns
    # Ensure all expected columns exist, handle missing ones gracefully
    columns_to_keep = [col for col in RELEVANT_COLUMNS if col in combined_df.columns]
    missing_cols = set(RELEVANT_COLUMNS) - set(columns_to_keep)
    if missing_cols:
        logging.warning(f"Missing expected columns in combined data: {missing_cols}. These will not be in the output.")

    if not ('latitude' in columns_to_keep and 'longitude' in columns_to_keep):
        logging.error("Essential 'latitude' or 'longitude' columns are missing. Cannot create map data.")
        return

    filtered_df = combined_df[columns_to_keep].copy() # Create a copy to avoid SettingWithCopyWarning

    # Filter out rows without valid latitude/longitude
    initial_rows = len(filtered_df)
    filtered_df.dropna(subset=['latitude', 'longitude'], inplace=True)
    rows_dropped = initial_rows - len(filtered_df)
    if rows_dropped > 0:
        logging.info(f"Dropped {rows_dropped} rows with missing latitude or longitude.")

    # Ensure lat/lon are numeric (they should be, but good to check)
    filtered_df['latitude'] = pd.to_numeric(filtered_df['latitude'], errors='coerce')
    filtered_df['longitude'] = pd.to_numeric(filtered_df['longitude'], errors='coerce')
    # Drop again if coercion failed
    filtered_df.dropna(subset=['latitude', 'longitude'], inplace=True)

    if filtered_df.empty:
        logging.warning("No valid geocoded data remaining after filtering. Output JSON will be empty.")
        output_data = []
    else:
        logging.info(f"{len(filtered_df)} valid geocoded records remaining for website.")
        # Convert DataFrame to list of dictionaries (records format)
        output_data = filtered_df.to_dict(orient='records')

    # Create output directory if it doesn't exist
    os.makedirs(WEBSITE_DATA_DIR, exist_ok=True)
    logging.info(f"Ensured website data directory exists: {WEBSITE_DATA_DIR}")

    # Save as JSON
    try:
        with open(OUTPUT_JSON_PATH, 'w') as f:
            json.dump(output_data, f, indent=2) # Use indent for readability (optional)
        logging.info(f"Successfully saved combined and filtered data to: {OUTPUT_JSON_PATH}")
    except Exception as e:
        logging.error(f"Error saving JSON file {OUTPUT_JSON_PATH}: {e}")

if __name__ == "__main__":
    prepare_data_for_website() 