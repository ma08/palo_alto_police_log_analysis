import os
import glob
import pandas as pd
import json
import logging
import re
from datetime import datetime

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
    'offense_category',
    'location', # Original location string
    'latitude',
    'longitude',
    'formatted_address',
    'google_maps_uri',
    'place_types',
    'location_interpretation'
]

def extract_date_from_filename(filename):
    """
    Extract date from filenames like 'april-07-2025-police-report-log_geocoded.csv'
    Returns a tuple of (original_date_string, parsed_datetime_object)
    """
    # Extract date part using regex
    # Updated regex to handle both _geocoded.csv and _processed.csv
    match = re.search(r'([a-z]+-\d+-\d+).*(?:_geocoded|_processed)\.csv', os.path.basename(filename).lower())
    if not match:
        # Try matching without the suffixes if the first attempt fails
        match = re.search(r'([a-z]+-\d+-\d+)', os.path.basename(filename).lower())
        if not match:
             logging.warning(f"Could not extract date string from filename: {filename}")
             return None, None

    date_str = match.group(1)  # e.g., "april-07-2025"
    
    try:
        # Parse the date
        date_parts = date_str.split('-')
        if len(date_parts) != 3:
            return date_str, None
            
        month_name, day, year = date_parts
        # Convert month name to number
        datetime_obj = datetime.strptime(f"{month_name} {day} {year}", "%B %d %Y")
        return date_str, datetime_obj
    except ValueError:
        # If parsing fails, return original string but no datetime object
        return date_str, None

def prepare_data_for_website():
    """Loads processed CSVs, combines them, filters, and saves as JSON for the website."""
    logging.info(f"Looking for processed CSV files in: {PROCESSED_CSV_DIR}")
    # Changed glob pattern to find *_processed.csv
    csv_files = glob.glob(os.path.join(PROCESSED_CSV_DIR, '*_processed.csv'))

    if not csv_files:
        # Updated error message
        logging.error(f"No *_processed.csv files found in {PROCESSED_CSV_DIR}. Cannot prepare website data. Did process_all_csvs.py run successfully?")
        return

    # Updated log message
    logging.info(f"Found {len(csv_files)} processed CSV files to combine.")

    all_data_frames = []
    for f in csv_files:
        try:
            df = pd.read_csv(f)
            # Extract police report date from filename
            report_date_str, report_datetime = extract_date_from_filename(f)
            
            # Add as new columns to the dataframe
            df['police_record_date_str'] = report_date_str  # Original string like "april-07-2025"
            
            # Add formatted date if datetime parsing was successful
            if report_datetime:
                df['police_record_date'] = report_datetime.strftime("%B %d, %Y")  # Formatted like "April 07, 2025"
            else:
                df['police_record_date'] = None
                
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
    logging.info(f"Combined data contains {len(combined_df)} total records before deduplication.")
    initial_count = len(combined_df)
    # Define columns to identify unique incidents
    deduplication_columns = ['case_number', 'date']
    # Ensure the columns exist before trying to deduplicate
    if all(col in combined_df.columns for col in deduplication_columns):
        combined_df.drop_duplicates(subset=deduplication_columns, keep='first', inplace=True)
        deduplicated_count = len(combined_df)
        records_removed = initial_count - deduplicated_count
        if records_removed > 0:
             logging.info(f"Removed {records_removed} duplicate records based on {', '.join(deduplication_columns)}.")
        else:
             logging.info(f"No duplicate records found based on {', '.join(deduplication_columns)}.")
    else:
        logging.warning(f"Cannot perform deduplication. Missing one or more key columns: {deduplication_columns}")

    # Ensure necessary columns for the website exist, including the new category
    required_web_cols = ['latitude', 'longitude', 'case_number', 'offense_category']
    missing_web_cols = [col for col in required_web_cols if col not in combined_df.columns]
    if missing_web_cols:
        logging.error(f"Essential columns for website ({', '.join(missing_web_cols)}) are missing in combined data. Cannot create website JSON.")
        return

    # Select relevant columns
    # Ensure all expected columns exist, handle missing ones gracefully
    columns_to_keep = [col for col in RELEVANT_COLUMNS if col in combined_df.columns]
    # Add the new police record date columns
    if 'police_record_date_str' in combined_df.columns:
        columns_to_keep.append('police_record_date_str')
    if 'police_record_date' in combined_df.columns:
        columns_to_keep.append('police_record_date')
        
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
        # Log based on the correct filtered data
        logging.info(f"{len(filtered_df)} valid records with lat/lon remaining for website.")
        # Convert DataFrame to list of dictionaries (records format)
        # Handle potential NaN values before converting to JSON
        # Convert specific pandas types (like NA) to None for JSON compatibility
        output_data = filtered_df.where(pd.notnull(filtered_df), None).to_dict(orient='records')

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