import os
import requests
import json
import logging
from dotenv import load_dotenv

# Configure logging if not already configured by the main script
# (using a basic config here for standalone use or testing)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load API key - Ensure .env file is in the project root
# This might be loaded again by the main script, which is fine.
load_dotenv()
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

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
        # This check is important even if loaded globally, in case it wasn't found
        logging.error("GOOGLE_MAPS_API_KEY is missing.")
        return None

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": fields,
    }

    data = {
        "textQuery": text_query,
        "locationBias": {
          "circle": {
            "center": {
              "latitude": 37.4419, # Approx center of Palo Alto
              "longitude": -122.1430
            },
            "radius": 15000.0 # 15km radius
          }
        }
        # Add other parameters like languageCode or regionCode if needed
    }

    logging.debug(f"Sending Places API request for query: '{text_query}'")
    response = None
    try:
        response = requests.post(TEXT_SEARCH_URL, headers=headers, json=data, timeout=15) # Increased timeout slightly
        response.raise_for_status()
        logging.debug(f"API Response Status Code for '{text_query}': {response.status_code}")
        return response.json()
    except requests.exceptions.Timeout:
        logging.error(f"API request timed out for query: '{text_query}'.")
        return None
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred for query '{text_query}': {http_err}")
        if response is not None:
            logging.error(f"Error Response Body: {response.text}")
        return None
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Error during API request for query '{text_query}': {req_err}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON response for query '{text_query}'.")
        if response is not None:
            logging.error(f"Raw Response Text: {response.text}")
        return None

def interpret_place_types(place_types: list[str]) -> str:
    """
    Provides a simple interpretation based on Google Places API types.

    Args:
        place_types: A list of type strings from the API response.

    Returns:
        A string category like 'intersection', 'street_address', 'route',
        'specific_place', or 'general_area/other'. Returns 'unknown' if
        input is empty or invalid.
    """
    if not place_types or not isinstance(place_types, list):
        return "unknown"

    if 'intersection' in place_types:
        return "intersection"
    # `premise` often accompanies `street_address` but is more specific
    if 'street_address' in place_types or 'premise' in place_types:
         # Prioritize 'premise' if both exist? For now, treat them similarly.
        return "street_address_or_premise"
    if 'route' in place_types:
        # Could be a highway or a street name without a specific number/intersection
        return "route"

    # Check for common types indicating a specific named place
    specific_place_types = {
        'establishment', 'point_of_interest', 'store', 'restaurant', 'park',
        'school', 'hospital', 'church', 'library', 'museum', 'airport',
        'shopping_mall', 'university', 'transit_station', 'gas_station',
        'lodging', # Hotels etc.
        # Add more as needed based on observed data
    }
    if any(ptype in place_types for ptype in specific_place_types):
        return "specific_place"

    # If none of the above, categorize as general or other
    # Examples might include 'neighborhood', 'locality', 'political'
    return "general_area_or_other"

# Example usage (optional, for testing the module directly)
if __name__ == '__main__':
    print("Testing geocoding_utils...")
    if not API_KEY:
        print("ERROR: GOOGLE_MAPS_API_KEY not set in .env file. Cannot run tests.")
    else:
        test_queries = [
            "470 MATADERO AVE, Palo Alto, CA",
            "EL CAMINO REAL/MIDDLE AVE, Palo Alto, CA",
            "Stanford University, Palo Alto, CA",
            "NonExistentPlace123, Palo Alto, CA"
        ]
        for query in test_queries:
            print(f"--- Testing Query: {query} ---")
            result = search_place(query, API_KEY)
            if result:
                print(" Raw Response:")
                print(json.dumps(result, indent=2))
                places = result.get('places', [])
                if places:
                    types = places[0].get('types', [])
                    interpretation = interpret_place_types(types)
                    print(f" Extracted Types: {types}")
                    print(f" Interpretation: {interpretation}")
                else:
                    print(" No places found in result.")
            else:
                print(" Failed to get result.")
            print("-" * 20) 