import os
import requests
import json
import pandas as pd
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
# Ensure you have a .env file in the project root with GOOGLE_MAPS_API_KEY="YOUR_API_KEY"
load_dotenv()
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Define the API endpoint
TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

def search_place(text_query: str, api_key: str, fields: str = "places.displayName,places.formattedAddress,places.location,places.googleMapsUri") -> dict | None:
    """
    Performs a Google Places Text Search (New) request.

    Args:
        text_query: The text string to search for (e.g., address, place name).
                     It's recommended to add city/state context (e.g., ", Palo Alto, CA")
                     to the query for better results, especially for street intersections.
        api_key: Your Google Maps Platform API key.
        fields: A comma-separated string of fields to return (FieldMask).
                Defaults to fields relevant for location extraction.

    Returns:
        A dictionary containing the API response JSON, or None if an error occurred.
    """
    if not api_key:
        logging.error("GOOGLE_MAPS_API_KEY not found in environment variables or .env file.")
        return None

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": fields,
    }

    data = {
        "textQuery": text_query,
        # --- Optional Parameters ---
        # You can uncomment and modify these based on the documentation
        # "languageCode": "en",
        # "regionCode": "US",
        # Add location bias towards Palo Alto for better relevance
        "locationBias": {
          "circle": {
            "center": {
              # Approximate center of Palo Alto
              "latitude": 37.4419,
              "longitude": -122.1430
            },
            "radius": 15000.0 # 15km radius - adjust as needed
          }
        }
        # Or use locationRestriction to *only* get results within an area:
        # "locationRestriction": {
        #   "rectangle": {
        #     "low": {"latitude": 37.36, "longitude": -122.20},
        #     "high": {"latitude": 37.47, "longitude": -122.09}
        #   }
        # }
        # --- End Optional Parameters ---
    }

    logging.info(f"Sending request for query: '{text_query}'")
    # logging.debug(f"Request Headers: {headers}") # Uncomment for detailed debugging
    # logging.debug(f"Request Body: {json.dumps(data, indent=2)}") # Uncomment for detailed debugging

    response = None # Initialize response to None
    try:
        response = requests.post(TEXT_SEARCH_URL, headers=headers, json=data, timeout=10) # Added timeout
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        logging.info(f"API Response Status Code: {response.status_code}")
        return response.json()
    except requests.exceptions.Timeout:
        logging.error("API request timed out.")
        return None
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        if response is not None:
            logging.error(f"Error Response Body: {response.text}")
        return None
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Error during API request: {req_err}")
        return None
    except json.JSONDecodeError:
        logging.error("Error decoding JSON response.")
        if response is not None:
            logging.error(f"Raw Response Text: {response.text}")
        return None

def process_csv_locations(csv_path: str, api_key: str, num_samples: int = 5):
    """
    Reads locations from a CSV, queries the Places API, and prints results.

    Args:
        csv_path: Path to the CSV file.
        api_key: Google Maps Platform API key.
        num_samples: Number of sample unique locations to process from the CSV.
                     Set to 0 or negative to process all unique locations.
    """
    try:
        # Assuming the script is run from the workspace root or notebooks folder
        # Adjust the path if necessary based on where you run it from.
        # If running from workspace root: 'data/csv_files/...'
        # If running from notebooks folder: '../data/csv_files/...'
        if not os.path.exists(csv_path):
             # Try adjusting path assuming script is in notebooks/
             adjusted_path = os.path.join('..', csv_path)
             if os.path.exists(adjusted_path):
                 csv_path = adjusted_path
             else:
                 logging.error(f"CSV file not found at {csv_path} or {adjusted_path}")
                 return

        df = pd.read_csv(csv_path)
        logging.info(f"Loaded CSV: {csv_path}")
        logging.info(f"Columns: {df.columns.tolist()}")

        if 'location' not in df.columns:
            logging.error(f"'location' column not found in {csv_path}")
            return

        # Get unique, non-null locations
        unique_locations = df['location'].dropna().unique()
        logging.info(f"Found {len(unique_locations)} unique non-null locations in the CSV.")

        if num_samples > 0 and len(unique_locations) > num_samples:
            sample_locations = unique_locations[:num_samples]
            logging.info(f"Processing the first {num_samples} unique locations.")
        else:
            sample_locations = unique_locations
            logging.info(f"Processing all {len(unique_locations)} unique locations.")


        print(f" --- Processing {len(sample_locations)} sample locations from CSV ---")

        results = {}
        for location_query in sample_locations:
             # Append "Palo Alto, CA" for better geographic context
             # Especially helpful for intersections or generic names
            full_query = f"{location_query}, Palo Alto, CA"
            print(f" --- Querying for: '{location_query}' (using query: '{full_query}') ---")
            result = search_place(full_query, api_key)

            if result:
                print(" Raw JSON Response:")
                print(json.dumps(result, indent=2))
                results[location_query] = result # Store result if needed
            else:
                print("Failed to get results for this location.")
                results[location_query] = None # Store None if failed

            print("-" * 30) # Separator between queries
        return results # Optionally return all results

    except FileNotFoundError:
        # This case is handled above, but kept for robustness
        logging.error(f"Error: CSV file not found at {csv_path}")
    except pd.errors.EmptyDataError:
        logging.error(f"Error: CSV file is empty: {csv_path}")
    except Exception as e:
        logging.error(f"An error occurred while processing the CSV: {e}", exc_info=True)


if __name__ == "__main__":
    if not API_KEY:
        print(" ERROR: GOOGLE_MAPS_API_KEY not set.")
        print("Please create a '.env' file in the project root directory")
        print("Add the following line to it, replacing YOUR_API_KEY with your actual key:")
        print('GOOGLE_MAPS_API_KEY="YOUR_API_KEY"')
        print(" Ensure you have installed the required packages: pip install requests python-dotenv pandas")
    else:
        # --- Example 1: Simple Text Query ---
        print(" " + "="*10 + " Example 1: Simple Text Query " + "="*10)
        # Using an address from the CSV sample
        query1 = "470 MATADERO AVE, Palo Alto, CA"
        result1 = search_place(query1, API_KEY)
        if result1:
            print(f" Raw JSON Response for Example 1 ('{query1}'):")
            print(json.dumps(result1, indent=2))
        else:
             print(f" Failed to get result for Example 1 ('{query1}')")
        print("-" * 40)


        # --- Example 2: Process Locations from CSV ---
        # Uses the path relative to the workspace root
        print(" " + "="*10 + " Example 2: Process Locations from CSV " + "="*10)
        csv_file_path = "data/csv_files/april-01-2025-police-report-log.csv"
        # Process first 3 unique non-null locations found in the CSV
        process_csv_locations(csv_file_path, API_KEY, num_samples=3)

        print(" Script finished.")
        print("You can now modify the 'search_place' function (e.g., change 'fields', add parameters)")
        print('or adjust the calls in the `if __name__ == "__main__":` block to experiment further.')


# Example output:
#  ========== Example 1: Simple Text Query ==========
# 2025-04-20 18:28:59,398 - INFO - Sending request for query: '470 MATADERO AVE, Palo Alto, CA'
# 2025-04-20 18:28:59,639 - INFO - API Response Status Code: 200
#  Raw JSON Response for Example 1 ('470 MATADERO AVE, Palo Alto, CA'):
# {
#   "places": [
#     {
#       "formattedAddress": "470 Matadero Ave, Palo Alto, CA 94306, USA",
#       "location": {
#         "latitude": 37.419548,
#         "longitude": -122.13339209999998
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=15734665055923993868",
#       "displayName": {
#         "text": "470 Matadero Ave"
#       }
#     }
#   ]
# }
# ----------------------------------------
#  ========== Example 2: Process Locations from CSV ==========
# 2025-04-20 18:28:59,646 - INFO - Loaded CSV: data/csv_files/april-01-2025-police-report-log.csv
# 2025-04-20 18:28:59,646 - INFO - Columns: ['case_number', 'date', 'time', 'offense_type', 'location', 'arrest_info']
# 2025-04-20 18:28:59,649 - INFO - Found 23 unique non-null locations in the CSV.
# 2025-04-20 18:28:59,649 - INFO - Processing the first 3 unique locations.
#  --- Processing 3 sample locations from CSV ---
#  --- Querying for: 'EL CAMINO REAL/MIDDLE AVE' (using query: 'EL CAMINO REAL/MIDDLE AVE, Palo Alto, CA') ---
# 2025-04-20 18:28:59,649 - INFO - Sending request for query: 'EL CAMINO REAL/MIDDLE AVE, Palo Alto, CA'
# 2025-04-20 18:28:59,869 - INFO - API Response Status Code: 200
#  Raw JSON Response:
# {
#   "places": [
#     {
#       "formattedAddress": "El Camino Real & Middle Ave, Menlo Park, CA 94025, USA",
#       "location": {
#         "latitude": 37.4504117,
#         "longitude": -122.1772462
#       },
#       "googleMapsUri": "https://maps.google.com/?q=El+Camino+Real+%26+Middle+Ave,+Menlo+Park,+CA+94025,+USA&ftid=0x808fa4b374673f95:0x11cd9af3a6572b43",
#       "displayName": {
#         "text": "El Camino Real & Middle Avenue",
#         "languageCode": "en"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: 'GALVEZ ST/EL CAMINO REAL' (using query: 'GALVEZ ST/EL CAMINO REAL, Palo Alto, CA') ---
# 2025-04-20 18:28:59,869 - INFO - Sending request for query: 'GALVEZ ST/EL CAMINO REAL, Palo Alto, CA'
# 2025-04-20 18:29:00,320 - INFO - API Response Status Code: 200
#  Raw JSON Response:
# {
#   "places": [
#     {
#       "formattedAddress": "El Camino Real & Galvez St, Palo Alto, CA 94301, USA",
#       "location": {
#         "latitude": 37.4373187,
#         "longitude": -122.1602327
#       },
#       "googleMapsUri": "https://maps.google.com/?q=El+Camino+Real+%26+Galvez+St,+Palo+Alto,+CA+94301,+USA&ftid=0x808fbb2451e36ec1:0x38e741690fdc138d",
#       "displayName": {
#         "text": "El Camino Real & Galvez Street",
#         "languageCode": "en"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '470 MATADERO AVE' (using query: '470 MATADERO AVE, Palo Alto, CA') ---
# 2025-04-20 18:29:00,321 - INFO - Sending request for query: '470 MATADERO AVE, Palo Alto, CA'
# 2025-04-20 18:29:00,492 - INFO - API Response Status Code: 200
#  Raw JSON Response:
# {
#   "places": [
#     {
#       "formattedAddress": "470 Matadero Ave, Palo Alto, CA 94306, USA",
#       "location": {
#         "latitude": 37.419548,
#         "longitude": -122.13339209999998
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=15734665055923993868",
#       "displayName": {
#         "text": "470 Matadero Ave"
#       }
#     }
#   ]
# }
# ------------------------------
#  Script finished.