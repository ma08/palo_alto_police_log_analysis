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

def search_place(text_query: str, api_key: str, fields: str = "places.displayName,places.formattedAddress,places.location,places.googleMapsUri,places.types") -> dict | None:
    """
    Performs a Google Places Text Search (New) request.

    Args:
        text_query: The text string to search for (e.g., address, place name).
                     It's recommended to add city/state context (e.g., ", Palo Alto, CA")
                     to the query for better results, especially for street intersections.
        api_key: Your Google Maps Platform API key.
        fields: A comma-separated string of fields to return (FieldMask).
                Defaults to fields relevant for location extraction including types.

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
    Reads locations from a CSV, queries the Places API, and prints results including type analysis.

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
                places = result.get('places', [])
                if places:
                    # Analyze the first result (usually the most relevant)
                    first_place = places[0]
                    place_types = first_place.get('types', [])
                    print(f" Extracted Types: {place_types}")

                    # --- Takeaway --- 
                    # The `places.types` field is useful for distinguishing location specificity.
                    # - `street_address` or `premise`: Indicates a specific building/address.
                    # - `intersection`: Indicates a road intersection.
                    # - `route`: Indicates a named street/road (broader than an intersection).
                    # - Other types (e.g., `establishment`, `point_of_interest`, `park`) indicate specific named places.
                    # This logic can be used in a pipeline to categorize geocoded results.
                    # ----------------

                    # Simple interpretation
                    if 'intersection' in place_types:
                        print(" Interpretation: Likely an intersection.")
                    elif 'route' in place_types:
                        print(" Interpretation: Likely a street/route.")
                    elif 'street_address' in place_types or 'premise' in place_types:
                        print(" Interpretation: Likely a specific address/premise.")
                    else:
                        # Check for common specific types
                        specific_types = ['establishment', 'point_of_interest', 'store', 'restaurant', 'park']
                        if any(ptype in place_types for ptype in specific_types):
                            print(" Interpretation: Likely a specific place/establishment.")
                        else:
                            print(" Interpretation: Type suggests a general area or less specific feature.")
                else:
                    print(" No places found in the result.")

                print("\n Raw JSON Response:")
                print(json.dumps(result, indent=2))
                results[location_query] = result # Store result if needed
            else:
                print(" Failed to get results for this location.")
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
        # --- Example 1: Simple Text Query (now includes types) ---
        print("\n" + "="*10 + " Example 1: Simple Text Query " + "="*10)
        # Using an address from the CSV sample
        query1 = "470 MATADERO AVE, Palo Alto, CA"
        result1 = search_place(query1, API_KEY)
        if result1:
            print(f"\n Raw JSON Response for Example 1 ('{query1}'):")
            places1 = result1.get('places', [])
            if places1:
                first_place1 = places1[0]
                place_types1 = first_place1.get('types', [])
                print(f" Extracted Types: {place_types1}")
                if 'intersection' in place_types1:
                    print(" Interpretation: Likely an intersection.")
                elif 'route' in place_types1:
                    print(" Interpretation: Likely a street/route.")
                elif 'street_address' in place_types1 or 'premise' in place_types1:
                    print(" Interpretation: Likely a specific address/premise.")
                else:
                    specific_types = ['establishment', 'point_of_interest', 'store', 'restaurant', 'park']
                    if any(ptype in place_types1 for ptype in specific_types):
                        print(" Interpretation: Likely a specific place/establishment.")
                    else:
                        print(" Interpretation: Type suggests a general area or less specific feature.")
            else:
                 print(" No places found in the result.")
            print("\n Raw JSON:")
            print(json.dumps(result1, indent=2))
        else:
             print(f"\n Failed to get result for Example 1 ('{query1}')")
        print("-" * 40)


        # --- Example 2: Process Locations from CSV (now includes type analysis) ---
        # Uses the path relative to the workspace root
        print("\n" + "="*10 + " Example 2: Process Locations from CSV " + "="*10)
        csv_file_path = "data/csv_files/april-01-2025-police-report-log.csv"
        # Process first 20 unique non-null locations found in the CSV to see more variety
        process_csv_locations(csv_file_path, API_KEY, num_samples=20)

        print("\n Script finished.")
        print("You can now modify the 'search_place' function (e.g., change 'fields', add parameters)")
        print('or adjust the calls in the `if __name__ == "__main__":` block to experiment further.')


# Example output:
# ========== Example 1: Simple Text Query ==========
# 2025-04-20 18:41:32,356 - INFO - Sending request for query: '470 MATADERO AVE, Palo Alto, CA'
# 2025-04-20 18:41:32,541 - INFO - API Response Status Code: 200

#  Raw JSON Response for Example 1 ('470 MATADERO AVE, Palo Alto, CA'):
#  Extracted Types: ['premise', 'street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON:
# {
#   "places": [
#     {
#       "types": [
#         "premise",
#         "street_address"
#       ],
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

# ========== Example 2: Process Locations from CSV ==========
# 2025-04-20 18:41:32,547 - INFO - Loaded CSV: data/csv_files/april-01-2025-police-report-log.csv
# 2025-04-20 18:41:32,547 - INFO - Columns: ['case_number', 'date', 'time', 'offense_type', 'location', 'arrest_info']
# 2025-04-20 18:41:32,549 - INFO - Found 23 unique non-null locations in the CSV.
# 2025-04-20 18:41:32,549 - INFO - Processing the first 20 unique locations.
#  --- Processing 20 sample locations from CSV ---
#  --- Querying for: 'EL CAMINO REAL/MIDDLE AVE' (using query: 'EL CAMINO REAL/MIDDLE AVE, Palo Alto, CA') ---
# 2025-04-20 18:41:32,549 - INFO - Sending request for query: 'EL CAMINO REAL/MIDDLE AVE, Palo Alto, CA'
# 2025-04-20 18:41:32,765 - INFO - API Response Status Code: 200
#  Extracted Types: ['intersection']
#  Interpretation: Likely an intersection.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "intersection"
#       ],
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
# 2025-04-20 18:41:32,766 - INFO - Sending request for query: 'GALVEZ ST/EL CAMINO REAL, Palo Alto, CA'
# 2025-04-20 18:41:33,134 - INFO - API Response Status Code: 200
#  Extracted Types: ['intersection']
#  Interpretation: Likely an intersection.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "intersection"
#       ],
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
# 2025-04-20 18:41:33,134 - INFO - Sending request for query: '470 MATADERO AVE, Palo Alto, CA'
# 2025-04-20 18:41:33,294 - INFO - API Response Status Code: 200
#  Extracted Types: ['premise', 'street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "premise",
#         "street_address"
#       ],
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
#  --- Querying for: '2452 WATSON CT' (using query: '2452 WATSON CT, Palo Alto, CA') ---
# 2025-04-20 18:41:33,294 - INFO - Sending request for query: '2452 WATSON CT, Palo Alto, CA'
# 2025-04-20 18:41:33,505 - INFO - API Response Status Code: 200
#  Extracted Types: ['premise', 'street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "premise",
#         "street_address"
#       ],
#       "formattedAddress": "2452 Watson Ct, Palo Alto, CA 94303, USA",
#       "location": {
#         "latitude": 37.4484607,
#         "longitude": -122.1205821
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=8365710209272835743",
#       "displayName": {
#         "text": "2452 Watson Ct"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '855 EL CAMINO REAL' (using query: '855 EL CAMINO REAL, Palo Alto, CA') ---
# 2025-04-20 18:41:33,505 - INFO - Sending request for query: '855 EL CAMINO REAL, Palo Alto, CA'
# 2025-04-20 18:41:33,848 - INFO - API Response Status Code: 200
#  Extracted Types: ['street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "street_address"
#       ],
#       "formattedAddress": "855 El Camino Real, Palo Alto, CA 94301, USA",
#       "location": {
#         "latitude": 37.4389939,
#         "longitude": -122.1588204
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=11118077703284017645",
#       "displayName": {
#         "text": "855 El Camino Real"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '722 ASHBY DR' (using query: '722 ASHBY DR, Palo Alto, CA') ---
# 2025-04-20 18:41:33,849 - INFO - Sending request for query: '722 ASHBY DR, Palo Alto, CA'
# 2025-04-20 18:41:34,202 - INFO - API Response Status Code: 200
#  Extracted Types: ['premise', 'street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "premise",
#         "street_address"
#       ],
#       "formattedAddress": "722 Ashby Dr, Palo Alto, CA 94301, USA",
#       "location": {
#         "latitude": 37.4524093,
#         "longitude": -122.14256669999999
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=16946665163692467534",
#       "displayName": {
#         "text": "722 Ashby Dr"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '1000 TANLAND DR' (using query: '1000 TANLAND DR, Palo Alto, CA') ---
# 2025-04-20 18:41:34,202 - INFO - Sending request for query: '1000 TANLAND DR, Palo Alto, CA'
# 2025-04-20 18:41:34,534 - INFO - API Response Status Code: 200
#  Extracted Types: ['street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "street_address"
#       ],
#       "formattedAddress": "1000 Tanland Dr, Palo Alto, CA 94303, USA",
#       "location": {
#         "latitude": 37.4429522,
#         "longitude": -122.12056020000001
#       },
#       "googleMapsUri": "https://maps.google.com/?q=1000+Tanland+Dr,+Palo+Alto,+CA+94303,+USA&ftid=0x808fbbacdd9e383b:0x55bdd7507ad7a0ff",
#       "displayName": {
#         "text": "1000 Tanland Dr"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '480 E MEADOW DR' (using query: '480 E MEADOW DR, Palo Alto, CA') ---
# 2025-04-20 18:41:34,534 - INFO - Sending request for query: '480 E MEADOW DR, Palo Alto, CA'
# 2025-04-20 18:41:34,885 - INFO - API Response Status Code: 200
#  Extracted Types: ['premise', 'street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "premise",
#         "street_address"
#       ],
#       "formattedAddress": "480 E Meadow Dr, Palo Alto, CA 94306, USA",
#       "location": {
#         "latitude": 37.4213437,
#         "longitude": -122.11868120000001
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=22357886726615768",
#       "displayName": {
#         "text": "480 E Meadow Dr"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '100 SANTA RITA AVE' (using query: '100 SANTA RITA AVE, Palo Alto, CA') ---
# 2025-04-20 18:41:34,886 - INFO - Sending request for query: '100 SANTA RITA AVE, Palo Alto, CA'
# 2025-04-20 18:41:35,233 - INFO - API Response Status Code: 200
#  Extracted Types: ['street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "street_address"
#       ],
#       "formattedAddress": "100 Santa Rita Ave, Palo Alto, CA 94301, USA",
#       "location": {
#         "latitude": 37.4309462,
#         "longitude": -122.14411790000001
#       },
#       "googleMapsUri": "https://maps.google.com/?q=100+Santa+Rita+Ave,+Palo+Alto,+CA+94301,+USA&ftid=0x808fbae38462280b:0x4270f461eaa7d062",
#       "displayName": {
#         "text": "100 Santa Rita Ave"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '180 EL CAMINO REAL' (using query: '180 EL CAMINO REAL, Palo Alto, CA') ---
# 2025-04-20 18:41:35,233 - INFO - Sending request for query: '180 EL CAMINO REAL, Palo Alto, CA'
# 2025-04-20 18:41:35,428 - INFO - API Response Status Code: 200
#  Extracted Types: ['street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "street_address"
#       ],
#       "formattedAddress": "180 El Camino Real, Palo Alto, CA 94304, USA",
#       "location": {
#         "latitude": 37.443646199999996,
#         "longitude": -122.16849889999997
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=10694182497936204464",
#       "displayName": {
#         "text": "180 El Camino Real"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '650 ADDISON AVE' (using query: '650 ADDISON AVE, Palo Alto, CA') ---
# 2025-04-20 18:41:35,428 - INFO - Sending request for query: '650 ADDISON AVE, Palo Alto, CA'
# 2025-04-20 18:41:35,605 - INFO - API Response Status Code: 200
#  Extracted Types: ['premise', 'street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "premise",
#         "street_address"
#       ],
#       "formattedAddress": "650 Addison Ave, Palo Alto, CA 94301, USA",
#       "location": {
#         "latitude": 37.4459643,
#         "longitude": -122.1508148
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=5219294379775897708",
#       "displayName": {
#         "text": "650 Addison Ave"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '879 LOMA VERDE AVE' (using query: '879 LOMA VERDE AVE, Palo Alto, CA') ---
# 2025-04-20 18:41:35,606 - INFO - Sending request for query: '879 LOMA VERDE AVE, Palo Alto, CA'
# 2025-04-20 18:41:35,887 - INFO - API Response Status Code: 200
#  Extracted Types: ['premise', 'street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "premise",
#         "street_address"
#       ],
#       "formattedAddress": "879 Loma Verde Ave, Palo Alto, CA 94303, USA",
#       "location": {
#         "latitude": 37.432808099999995,
#         "longitude": -122.117804
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=16012634385170397325",
#       "displayName": {
#         "text": "879 Loma Verde Ave"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '2775 EMBARCADERO RD' (using query: '2775 EMBARCADERO RD, Palo Alto, CA') ---
# 2025-04-20 18:41:35,888 - INFO - Sending request for query: '2775 EMBARCADERO RD, Palo Alto, CA'
# 2025-04-20 18:41:36,149 - INFO - API Response Status Code: 200
#  Extracted Types: ['premise', 'street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "premise",
#         "street_address"
#       ],
#       "formattedAddress": "2775 Embarcadero Rd, Palo Alto, CA 94303, USA",
#       "location": {
#         "latitude": 37.459628099999996,
#         "longitude": -122.1063941
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=16863978290233558804",
#       "displayName": {
#         "text": "2775 Embarcadero Rd"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '2350 BIRCH ST' (using query: '2350 BIRCH ST, Palo Alto, CA') ---
# 2025-04-20 18:41:36,150 - INFO - Sending request for query: '2350 BIRCH ST, Palo Alto, CA'
# 2025-04-20 18:41:36,357 - INFO - API Response Status Code: 200
#  Extracted Types: ['street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "street_address"
#       ],
#       "formattedAddress": "2350 Birch St, Palo Alto, CA 94306, USA",
#       "location": {
#         "latitude": 37.42768040000001,
#         "longitude": -122.14446339999999
#       },
#       "googleMapsUri": "https://maps.google.com/?q=2350+Birch+St,+Palo+Alto,+CA+94306,+USA&ftid=0x808fbae5b7214d77:0xa97265441ad27b47",
#       "displayName": {
#         "text": "2350 Birch St"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '757 TENNYSON AVE' (using query: '757 TENNYSON AVE, Palo Alto, CA') ---
# 2025-04-20 18:41:36,358 - INFO - Sending request for query: '757 TENNYSON AVE, Palo Alto, CA'
# 2025-04-20 18:41:36,546 - INFO - API Response Status Code: 200
#  Extracted Types: ['premise', 'street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "premise",
#         "street_address"
#       ],
#       "formattedAddress": "757 Tennyson Ave, Palo Alto, CA 94303, USA",
#       "location": {
#         "latitude": 37.4415589,
#         "longitude": -122.13997040000001
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=9296510258926984932",
#       "displayName": {
#         "text": "757 Tennyson Ave"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: 'EL CAMINO REAL' (using query: 'EL CAMINO REAL, Palo Alto, CA') ---
# 2025-04-20 18:41:36,547 - INFO - Sending request for query: 'EL CAMINO REAL, Palo Alto, CA'
# 2025-04-20 18:41:36,977 - INFO - API Response Status Code: 200
#  Extracted Types: ['route']
#  Interpretation: Likely a street/route.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "route"
#       ],
#       "formattedAddress": "El Camino Real, Palo Alto, CA, USA",
#       "location": {
#         "latitude": 37.4203243,
#         "longitude": -122.1366538
#       },
#       "googleMapsUri": "https://maps.google.com/?q=El+Camino+Real,+Palo+Alto,+CA,+USA&ftid=0x808fcb0b782c489d:0x380db039ff6cae75",
#       "displayName": {
#         "text": "El Camino Real",
#         "languageCode": "en"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '356 IRIS WAY' (using query: '356 IRIS WAY, Palo Alto, CA') ---
# 2025-04-20 18:41:36,978 - INFO - Sending request for query: '356 IRIS WAY, Palo Alto, CA'
# 2025-04-20 18:41:37,410 - INFO - API Response Status Code: 200
#  Extracted Types: ['premise', 'street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "premise",
#         "street_address"
#       ],
#       "formattedAddress": "356 Iris Way, Palo Alto, CA 94303, USA",
#       "location": {
#         "latitude": 37.4482208,
#         "longitude": -122.13261039999999
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=13821446246599032561",
#       "displayName": {
#         "text": "356 Iris Way"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '2727 MIDTOWN CT' (using query: '2727 MIDTOWN CT, Palo Alto, CA') ---
# 2025-04-20 18:41:37,410 - INFO - Sending request for query: '2727 MIDTOWN CT, Palo Alto, CA'
# 2025-04-20 18:41:37,608 - INFO - API Response Status Code: 200
#  Extracted Types: ['premise', 'street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "premise",
#         "street_address"
#       ],
#       "formattedAddress": "2727 Midtown Ct, Palo Alto, CA 94303, USA",
#       "location": {
#         "latitude": 37.434506,
#         "longitude": -122.1275342
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=16824516795052090390",
#       "displayName": {
#         "text": "2727 Midtown Ct"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: '3000 ALEXIS DR' (using query: '3000 ALEXIS DR, Palo Alto, CA') ---
# 2025-04-20 18:41:37,608 - INFO - Sending request for query: '3000 ALEXIS DR, Palo Alto, CA'
# 2025-04-20 18:41:37,815 - INFO - API Response Status Code: 200
#  Extracted Types: ['premise', 'street_address']
#  Interpretation: Likely a specific address/premise.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "premise",
#         "street_address"
#       ],
#       "formattedAddress": "3000 Alexis Dr, Palo Alto, CA 94304, USA",
#       "location": {
#         "latitude": 37.3739753,
#         "longitude": -122.17195249999997
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=4769930203467316478",
#       "displayName": {
#         "text": "3000 Alexis Dr"
#       }
#     }
#   ]
# }
# ------------------------------
#  --- Querying for: 'BYRON ST' (using query: 'BYRON ST, Palo Alto, CA') ---
# 2025-04-20 18:41:37,815 - INFO - Sending request for query: 'BYRON ST, Palo Alto, CA'
# 2025-04-20 18:41:38,009 - INFO - API Response Status Code: 200
#  Extracted Types: ['route']
#  Interpretation: Likely a street/route.

#  Raw JSON Response:
# {
#   "places": [
#     {
#       "types": [
#         "route"
#       ],
#       "formattedAddress": "Byron St, Palo Alto, CA, USA",
#       "location": {
#         "latitude": 37.4351592,
#         "longitude": -122.1339506
#       },
#       "googleMapsUri": "https://maps.google.com/?cid=12326788350264137327",
#       "displayName": {
#         "text": "Byron Street",
#         "languageCode": "en"
#       }
#     }
#   ]
# }
# ------------------------------

#  Script finished.