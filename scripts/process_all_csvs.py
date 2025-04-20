import os
import glob
import pandas as pd
import logging
from dotenv import load_dotenv
from tqdm import tqdm # Import tqdm for progress bar
import time

# Ensure the src directory is discoverable (if running scripts/process_all_csvs.py directly)
# This might not be needed depending on your project structure and how you run the script
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.geocoding_utils import search_place, interpret_place_types

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file in the project root
load_dotenv()
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Define directories
INPUT_DIR = "data/csv_files"
OUTPUT_DIR = "data/processed_csv_files"
CACHE_FILE = "data/geocoding_cache.json" # Simple file-based cache

# --- Cache Functions ---
def load_cache(cache_path):
    """Loads the geocoding cache from a JSON file."""
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                logging.info(f"Loading cache from {cache_path}")
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Could not load cache file {cache_path}: {e}. Starting with empty cache.")
            return {}
    else:
        logging.info("No cache file found. Starting with empty cache.")
        return {}

def save_cache(cache_data, cache_path):
    """Saves the geocoding cache to a JSON file."""
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, 'w') as f:
            json.dump(cache_data, f, indent=2)
        logging.info(f"Saved cache to {cache_path}")
    except IOError as e:
        logging.error(f"Could not save cache file {cache_path}: {e}")

# --- Main Processing Function ---
def process_csv(csv_path, output_dir, api_key, geocoding_cache):
    """Processes a single CSV file to add geocoding data."""
    filename = os.path.basename(csv_path)
    output_path = os.path.join(output_dir, filename.replace(".csv", "_geocoded.csv"))

    logging.info(f"Processing {filename}...")

    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        logging.error(f"File not found: {csv_path}")
        return False # Indicate failure
    except pd.errors.EmptyDataError:
        logging.warning(f"File is empty: {csv_path}. Skipping.")
        return True # Indicate success (nothing to process)
    except Exception as e:
        logging.error(f"Error reading {csv_path}: {e}")
        return False # Indicate failure

    if 'location' not in df.columns:
        logging.warning(f"'location' column not found in {filename}. Skipping.")
        return True # Indicate success (nothing to process)

    # Drop rows where location is null/empty, as they cannot be geocoded
    original_count = len(df)
    df.dropna(subset=['location'], inplace=True)
    if len(df) < original_count:
        logging.info(f"Dropped {original_count - len(df)} rows with missing locations from {filename}.")

    if df.empty:
        logging.info(f"No valid locations to process in {filename} after dropping missing values. Skipping API calls.")
        # Still save an empty/header-only file if needed, or just skip saving?
        # Let's save it with potentially new columns but no data.
        df['latitude'] = None
        df['longitude'] = None
        df['formatted_address'] = None
        df['google_maps_uri'] = None
        df['place_types'] = None
        df['location_interpretation'] = None
        try:
            df.to_csv(output_path, index=False)
            logging.info(f"Saved empty geocoded file (due to no valid locations) to {output_path}")
        except Exception as e:
            logging.error(f"Error saving empty geocoded file {output_path}: {e}")
            return False
        return True


    unique_locations = df['location'].unique()
    logging.info(f"Found {len(unique_locations)} unique locations to geocode in {filename}.")

    results = {}
    api_calls_made = 0
    for loc in tqdm(unique_locations, desc=f"Geocoding {filename}", unit="location"):
        if pd.isna(loc) or not isinstance(loc, str) or not loc.strip():
            results[loc] = {'latitude': pd.NA, 'longitude': pd.NA, 'formatted_address': pd.NA,
                            'google_maps_uri': pd.NA, 'place_types': pd.NA, 'location_interpretation': 'invalid_input'}
            continue

        # Add context for better results
        full_query = f"{loc}, Palo Alto, CA"

        # Check cache first
        if full_query in geocoding_cache:
            api_result = geocoding_cache[full_query]
            logging.debug(f"Cache hit for: '{full_query}'")
        else:
            # Make API call
            logging.debug(f"Cache miss. Calling API for: '{full_query}'")
            api_result = search_place(full_query, api_key)
            api_calls_made += 1
            # Store result (even if None) in cache to avoid re-querying failures
            geocoding_cache[full_query] = api_result
            # Optional: Add a small delay to respect potential rate limits
            time.sleep(0.05) # 50ms delay - adjust as needed

        # Process the result
        if api_result and api_result.get('places'):
            first_place = api_result['places'][0]
            lat = first_place.get('location', {}).get('latitude')
            lon = first_place.get('location', {}).get('longitude')
            addr = first_place.get('formattedAddress')
            uri = first_place.get('googleMapsUri')
            types = first_place.get('types', [])
            interp = interpret_place_types(types)
            results[loc] = {'latitude': lat, 'longitude': lon, 'formatted_address': addr,
                            'google_maps_uri': uri, 'place_types': ",".join(types), # Store as comma-separated string
                            'location_interpretation': interp}
        else:
            # Handle cases where API returned no places or an error (api_result is None)
            logging.warning(f"No place found or API error for query: '{full_query}' (Original: '{loc}')")
            results[loc] = {'latitude': pd.NA, 'longitude': pd.NA, 'formatted_address': pd.NA,
                            'google_maps_uri': pd.NA, 'place_types': pd.NA, 'location_interpretation': 'not_found'}

    logging.info(f"Made {api_calls_made} API calls for {filename}.")

    # Map results back to the DataFrame
    df['latitude'] = df['location'].map(lambda x: results.get(x, {}).get('latitude'))
    df['longitude'] = df['location'].map(lambda x: results.get(x, {}).get('longitude'))
    df['formatted_address'] = df['location'].map(lambda x: results.get(x, {}).get('formatted_address'))
    df['google_maps_uri'] = df['location'].map(lambda x: results.get(x, {}).get('google_maps_uri'))
    df['place_types'] = df['location'].map(lambda x: results.get(x, {}).get('place_types'))
    df['location_interpretation'] = df['location'].map(lambda x: results.get(x, {}).get('location_interpretation'))

    # Save the processed DataFrame
    try:
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(output_path, index=False)
        logging.info(f"Successfully processed and saved geocoded data to {output_path}")
        return True # Indicate success
    except Exception as e:
        logging.error(f"Error saving processed file {output_path}: {e}")
        return False # Indicate failure

# --- Main Execution Block ---
if __name__ == "__main__":
    if not API_KEY:
        logging.error("FATAL: GOOGLE_MAPS_API_KEY not found in environment variables or .env file.")
        logging.error("Please ensure a .env file exists in the project root with your API key:")
        logging.error('GOOGLE_MAPS_API_KEY="YOUR_API_KEY"')
        sys.exit(1) # Exit if API key is missing

    logging.info("Starting CSV geocoding process...")
    logging.info(f"Input directory: {INPUT_DIR}")
    logging.info(f"Output directory: {OUTPUT_DIR}")
    logging.info(f"Cache file: {CACHE_FILE}")

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Find all CSV files in the input directory
    csv_files = glob.glob(os.path.join(INPUT_DIR, '*.csv'))

    if not csv_files:
        logging.warning(f"No CSV files found in {INPUT_DIR}. Exiting.")
        sys.exit(0)

    logging.info(f"Found {len(csv_files)} CSV files to process.")

    # Load existing cache
    import json # Ensure json is imported for cache functions
    geocoding_cache = load_cache(CACHE_FILE)
    initial_cache_size = len(geocoding_cache)

    processed_count = 0
    failed_count = 0
    for csv_file in csv_files:
        success = process_csv(csv_file, OUTPUT_DIR, API_KEY, geocoding_cache)
        if success:
            processed_count += 1
        else:
            failed_count += 1

    # Save updated cache only if it has changed
    if len(geocoding_cache) > initial_cache_size:
        save_cache(geocoding_cache, CACHE_FILE)
    else:
        logging.info("Cache not modified, skipping save.")

    logging.info("--- Geocoding Process Summary ---")
    logging.info(f"Successfully processed: {processed_count} files")
    logging.info(f"Failed to process: {failed_count} files")
    logging.info(f"Total unique locations cached: {len(geocoding_cache)}")
    logging.info("---------------------------------")

    if failed_count > 0:
        logging.warning("Some files failed during processing. Check logs above for details.")
        sys.exit(1)
    else:
        logging.info("All files processed successfully.")
        sys.exit(0) 