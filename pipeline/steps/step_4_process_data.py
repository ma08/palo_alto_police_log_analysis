import os
import re
import glob
import pandas as pd
import logging
from dotenv import load_dotenv
from tqdm import tqdm # Import tqdm for progress bar
import time
import json # Added for cache handling
from anthropic import AnthropicBedrock # Added for LLM calls

# Remove sys.path manipulation, rely on package structure
# import sys
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Assume geocoding_utils is correctly placed or path adjusted
try:
    # from src.geocoding_utils import search_place, interpret_place_types # Old import
    from pipeline.utils.geocoding import search_place, interpret_place_types # Corrected import
except ImportError:
    logging.error("Could not import geocoding_utils. Ensure pipeline/utils/geocoding.py exists.")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file in the project root
load_dotenv()
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY") # For Geocoding
# AWS credentials for Bedrock are expected to be in environment or ~/.aws/credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1") # Or your Bedrock region
# CLAUDE_MODEL_ID = os.getenv("CLAUDE_MODEL_ID", "us.anthropic.claude-3-7-sonnet-20250219-v1:0") # Specify desired model
CLAUDE_MODEL_ID = "us.anthropic.claude-3-5-sonnet-20241022-v2:0"

# Define directories
INPUT_DIR = "data/csv_files"
OUTPUT_DIR = "data/processed_csv_files"
GEOCODING_CACHE_FILE = "data/geocoding_cache.json" # Cache for geocoding results
OFFENSE_CATEGORY_CACHE_FILE = "data/offense_category_cache.json" # Cache for LLM categorization

# --- Finalized Offense Categories ---
OFFENSE_CATEGORIES = [
    "Theft",
    "Burglary",
    "Vehicle Crime",
    "Traffic Incidents",
    "Property Crime",
    "Violent/Person Crime",
    "Fraud/Financial Crime",
    "Public Order/Disturbance",
    "Warrant/Arrest",
    "Administrative/Other",
]

# --- Generic Cache Functions (Refactored) ---
def load_json_cache(cache_path, cache_name="Cache"):
    """Loads a cache from a JSON file."""
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                logging.info(f"Loading {cache_name} from {cache_path}")
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Could not load {cache_name} file {cache_path}: {e}. Starting with empty {cache_name}.")
            return {}
    else:
        logging.info(f"No {cache_name} file found at {cache_path}. Starting with empty {cache_name}.")
        return {}

def save_json_cache(cache_data, cache_path, cache_name="Cache"):
    """Saves a cache to a JSON file."""
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, 'w') as f:
            json.dump(cache_data, f, indent=2)
        logging.info(f"Saved {cache_name} to {cache_path}")
    except IOError as e:
        logging.error(f"Could not save {cache_name} file {cache_path}: {e}")


# --- LLM Categorization Function ---
def get_offense_categories_from_llm(offense_types_to_categorize, llm_client, categories, model_id):
    """
    Uses Bedrock Claude to categorize a list of offense types.

    Args:
        offense_types_to_categorize: List of unique offense type strings needing categorization.
        llm_client: Initialized AnthropicBedrock client.
        categories: The predefined list of target categories.
        model_id: The specific Claude model ID to use.

    Returns:
        A dictionary mapping input offense_type strings to their predicted category.
        Returns empty dict if input list is empty or an error occurs.
    """
    if not offense_types_to_categorize:
        return {}

    # Format the list for the prompt
    offense_list_str = "\\n".join([f"- {ot}" for ot in offense_types_to_categorize])
    category_list_str = "\\n".join([f"- {cat}" for cat in categories])

    prompt = f"""
You are an expert police report analyst. Your task is to categorize the following raw offense types into one of the predefined categories.

Predefined Categories:
{category_list_str}

Offense Types to Categorize:
{offense_list_str}

Please provide the categorization as a JSON object where keys are the raw offense types and values are the corresponding predefined category. Only output the JSON object, with no additional text, commentary, or explanation before or after the JSON. Ensure every offense type provided is included as a key in the JSON response.

Example Response Format:
{{
  "Raw Offense Type 1": "Chosen Category 1",
  "Raw Offense Type 2": "Chosen Category 2",
  ...
}}
"""

    messages = [{"role": "user", "content": prompt}]
    max_tokens = 2048 # Adjust if needed based on list size
    temperature = 0.0 # For deterministic categorization

    logging.info(f"Calling LLM to categorize {len(offense_types_to_categorize)} new offense types...")
    try:
        response = llm_client.messages.create(
            model=model_id,
            max_tokens=max_tokens,
            messages=messages,
            temperature=temperature
        )

        if response.content and len(response.content) > 0:
            response_text = response.content[0].text.strip()
            # Find JSON block, handling potential markdown ```json ... ```
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                category_map = json.loads(json_str)
                logging.info(f"LLM successfully categorized {len(category_map)} types.")

                # Validate response - check if all input types are keys in the map
                missing_keys = set(offense_types_to_categorize) - set(category_map.keys())
                if missing_keys:
                    logging.warning(f"LLM response missing keys for: {missing_keys}. Assigning 'Administrative/Other'.")
                    for key in missing_keys:
                        category_map[key] = "Administrative/Other" # Assign default

                # Validate response - check if assigned categories are valid
                invalid_assignments = {}
                for key, value in category_map.items():
                    if value not in categories:
                        invalid_assignments[key] = value
                if invalid_assignments:
                    logging.warning(f"LLM assigned invalid categories: {invalid_assignments}. Assigning 'Administrative/Other'.")
                    for key in invalid_assignments:
                         category_map[key] = "Administrative/Other" # Assign default

                return category_map
            else:
                 logging.exception(f"Could not extract valid JSON from LLM response: {response_text}")
                 return {} # Failed to parse
        else:
            logging.error("LLM returned empty content.")
            return {}
    except Exception as e:
        logging.exception(f"Error calling Bedrock API for categorization: {e}")
        # Fallback: Assign 'Administrative/Other' to all requested types on error
        return {ot: "Administrative/Other" for ot in offense_types_to_categorize}


# --- Main Processing Function (Updated) ---
def process_csv(csv_path, output_dir, api_key, geocoding_cache, llm_client, offense_category_cache):
    """Processes a single CSV file to add geocoding and offense category data."""
    filename = os.path.basename(csv_path)
    output_path = os.path.join(output_dir, filename.replace(".csv", "_processed.csv")) # Changed suffix

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

    required_columns = ['location', 'offense_type'] # Now require both
    missing_req_cols = [col for col in required_columns if col not in df.columns]
    if missing_req_cols:
        logging.warning(f"Missing required columns ({', '.join(missing_req_cols)}) in {filename}. Skipping.")
        return True # Skip file if essential columns missing

    # --- Offense Categorization ---
    df['offense_type'] = df['offense_type'].astype(str).str.strip() # Clean whitespace
    unique_offenses_in_file = df['offense_type'].dropna().unique()
    offenses_to_categorize = [
        offense for offense in unique_offenses_in_file
        if offense and offense not in offense_category_cache # Check cache
    ]

    if offenses_to_categorize:
        logging.info(f"Found {len(offenses_to_categorize)} unique offense types in {filename} needing categorization.")
        # Call LLM
        new_categories = get_offense_categories_from_llm(
            offenses_to_categorize, llm_client, OFFENSE_CATEGORIES, CLAUDE_MODEL_ID
        )
        # Update cache immediately
        offense_category_cache.update(new_categories)
        logging.info(f"Updated offense category cache with {len(new_categories)} new entries.")
    else:
         logging.debug(f"All offense types in {filename} already in cache.")

    # Add category column using the (potentially updated) cache
    df['offense_category'] = df['offense_type'].map(offense_category_cache).fillna("Administrative/Other")

    # --- Geocoding (remains largely the same, ensure columns added correctly) ---
    # Drop rows where location is null/empty before geocoding
    original_count = len(df)
    df.dropna(subset=['location'], inplace=True)
    if len(df) < original_count:
        logging.info(f"Dropped {original_count - len(df)} rows with missing locations from {filename}.")

    # Add placeholder columns before potential early exit if df becomes empty
    geo_cols = ['latitude', 'longitude', 'formatted_address', 'google_maps_uri', 'place_types', 'location_interpretation']
    for col in geo_cols:
        if col not in df.columns:
             df[col] = pd.NA # Use pandas NA for consistency

    if df.empty:
        logging.info(f"No valid locations to process in {filename} after dropping missing values. Skipping geocoding API calls.")
        # Save file with category column and empty geo columns
        try:
            os.makedirs(output_dir, exist_ok=True) # Ensure dir exists
            df.to_csv(output_path, index=False)
            logging.info(f"Saved processed file (no geocoding needed) to {output_path}")
        except Exception as e:
            logging.error(f"Error saving processed file {output_path}: {e}")
            return False
        return True

    # Proceed with geocoding for non-empty df
    unique_locations = df['location'].unique()
    logging.info(f"Found {len(unique_locations)} unique locations to geocode in {filename}.")

    geo_results = {}
    api_calls_made = 0
    for loc in tqdm(unique_locations, desc=f"Geocoding {filename}", unit="location"):
        if pd.isna(loc) or not isinstance(loc, str) or not loc.strip():
            geo_results[loc] = {col: pd.NA for col in geo_cols}
            geo_results[loc]['location_interpretation'] = 'invalid_input'
            continue

        full_query = f"{loc}, Palo Alto, CA"
        if full_query in geocoding_cache:
            api_result = geocoding_cache[full_query]
            # logging.debug(f"Cache hit for: '{full_query}'") # Can be verbose
        else:
            # logging.debug(f"Cache miss. Calling API for: '{full_query}'") # Can be verbose
            api_result = search_place(full_query, api_key)
            api_calls_made += 1
            geocoding_cache[full_query] = api_result
            time.sleep(0.05) # Rate limiting

        if api_result and api_result.get('places'):
            first_place = api_result['places'][0]
            types = first_place.get('types', [])
            geo_results[loc] = {
                'latitude': first_place.get('location', {}).get('latitude'),
                'longitude': first_place.get('location', {}).get('longitude'),
                'formatted_address': first_place.get('formattedAddress'),
                'google_maps_uri': first_place.get('googleMapsUri'),
                'place_types': ",".join(types),
                'location_interpretation': interpret_place_types(types)
            }
        else:
            # logging.warning(f"No place found or API error for query: '{full_query}' (Original: '{loc}')") # Can be verbose
            geo_results[loc] = {col: pd.NA for col in geo_cols}
            geo_results[loc]['location_interpretation'] = 'not_found'

    if api_calls_made > 0:
        logging.info(f"Made {api_calls_made} geocoding API calls for {filename}.")

    # Map geocoding results back efficiently
    for col in geo_cols:
         df[col] = df['location'].map(lambda x: geo_results.get(x, {}).get(col))

    # --- Save the final processed DataFrame ---
    try:
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(output_path, index=False)
        logging.info(f"Successfully processed and saved data to {output_path}")
        return True # Indicate success
    except Exception as e:
        logging.error(f"Error saving processed file {output_path}: {e}")
        return False # Indicate failure

# --- Main Processing Function (Encapsulated) ---
def run_processing():
    """Orchestrates the processing of all CSV files for geocoding and categorization."""
    import re # Import re for LLM response parsing

    # Check API key for Geocoding
    if not API_KEY:
        logging.error("FATAL: GOOGLE_MAPS_API_KEY not found in environment variables or .env file.")
        logging.error("Please ensure a .env file exists in the project root with your API key:")
        logging.error('GOOGLE_MAPS_API_KEY="YOUR_API_KEY"')
        # Raise an exception instead of exiting, so the orchestrator can handle it
        raise EnvironmentError("Missing GOOGLE_MAPS_API_KEY")

    # Initialize Bedrock Client for LLM Categorization
    llm_client = None # Initialize to None
    try:
        llm_client = AnthropicBedrock(
            aws_region=AWS_REGION,
            aws_access_key=AWS_ACCESS_KEY_ID,
            aws_secret_key=AWS_SECRET_ACCESS_KEY,
        )
        logging.info(f"Initialized AnthropicBedrock client for model {CLAUDE_MODEL_ID} in region {AWS_REGION}")
    except Exception as e:
        logging.error(f"FATAL: Failed to initialize AnthropicBedrock client: {e}")
        logging.error("Ensure AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) and region are configured correctly (environment variables or ~/.aws/credentials).")
        # Raise an exception
        raise ConnectionError(f"Failed to initialize Bedrock client: {e}")

    logging.info("Starting CSV processing (Geocoding and Categorization)...")
    logging.info(f"Input directory: {INPUT_DIR}")
    logging.info(f"Output directory: {OUTPUT_DIR}")
    logging.info(f"Geocoding Cache file: {GEOCODING_CACHE_FILE}")
    logging.info(f"Offense Category Cache file: {OFFENSE_CATEGORY_CACHE_FILE}")

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Find all CSV files in the input directory
    csv_files = glob.glob(os.path.join(INPUT_DIR, '*.csv'))

    if not csv_files:
        logging.warning(f"No CSV files found in {INPUT_DIR}. Skipping processing step.")
        return # Don't raise an error, just return if no files

    logging.info(f"Found {len(csv_files)} CSV files to process.")

    # Load existing caches
    geocoding_cache = load_json_cache(GEOCODING_CACHE_FILE, "Geocoding Cache")
    offense_category_cache = load_json_cache(OFFENSE_CATEGORY_CACHE_FILE, "Offense Category Cache")
    initial_geo_cache_size = len(geocoding_cache)
    initial_offense_cache_size = len(offense_category_cache)

    processed_count = 0
    failed_count = 0
    # Process files
    for csv_file in csv_files:
        success = process_csv(
            csv_file,
            OUTPUT_DIR,
            API_KEY,
            geocoding_cache,
            llm_client,
            offense_category_cache # Pass necessary caches and client
        )
        if success:
            processed_count += 1
        else:
            failed_count += 1
        # --- Save caches periodically or after each file? ---
        # Saving after each file is safer but slower. Saving at end is faster.
        # Let's save at the end for now for performance. Consider changing if script crashes often.

    # Save updated caches if they changed
    if len(geocoding_cache) > initial_geo_cache_size:
        save_json_cache(geocoding_cache, GEOCODING_CACHE_FILE, "Geocoding Cache")
    else:
        logging.info("Geocoding cache not modified, skipping save.")

    if len(offense_category_cache) > initial_offense_cache_size:
        save_json_cache(offense_category_cache, OFFENSE_CATEGORY_CACHE_FILE, "Offense Category Cache")
    else:
        logging.info("Offense category cache not modified, skipping save.")

    logging.info("--- Processing Summary ---")
    logging.info(f"Successfully processed: {processed_count} files")
    logging.info(f"Failed to process: {failed_count} files")
    logging.info(f"Total unique locations cached: {len(geocoding_cache)}")
    logging.info(f"Total unique offense types cached: {len(offense_category_cache)}")
    logging.info("--------------------------")

    if failed_count > 0:
        logging.warning("Some files failed during processing. Check logs above for details.")
        # Raise an exception to signal failure to the orchestrator
        raise RuntimeError(f"{failed_count} files failed during CSV processing.")
    else:
        logging.info("All files processed successfully.")
        # No need to exit or return anything specific on success

# --- Main Execution Block (Updated) ---
if __name__ == "__main__":
    try:
        run_processing()
        sys.exit(0) # Explicit success exit
    except Exception as e:
        logging.error(f"An error occurred during CSV processing: {e}")
        sys.exit(1) # Explicit failure exit 