#!/usr/bin/env python3
"""
download_reports.py - Downloads police reports from Palo Alto Police Department website for a given date range.
"""

import os
import datetime
import requests
# from bs4 import BeautifulSoup # No longer needed
from urllib.parse import urljoin
from tqdm import tqdm
import argparse
import sys

BASE_URL = "https://www.paloalto.gov"
# REPORT_LOG_URL = f"{BASE_URL}/departments/police/public-information-portal/police-report-log" # No longer used
# DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "raw") # Old path
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "raw_pdfs") # New path

def ensure_directory_exists(directory):
    """Ensure the specified directory exists."""
    os.makedirs(directory, exist_ok=True)

# def get_report_links(): # Removed - using URL generation
#     """Fetch the webpage and extract links to the last 30 days of police reports."""
#     try:
#         response = requests.get(REPORT_LOG_URL)
#         response.raise_for_status()
#         soup = BeautifulSoup(response.text, 'lxml')
#         
#         # Find all PDF links that match the pattern of police reports
#         links = []
#         for a_tag in soup.find_all('a', href=True):
#             href = a_tag['href']
#             if '-Police-Report-Log.pdf' in href:
#                 links.append(urljoin(BASE_URL, href))
#         
#         return links
#     except requests.RequestException as e:
#         print(f"Error fetching report links: {e}")
#         return []

def generate_report_urls(start_date, end_date):
    """Generate URLs for the specified date range."""
    urls = []
    current_date = start_date
    
    while current_date <= end_date:
        month = current_date.strftime("%B").lower()
        day = current_date.day
        year = current_date.year
        
        # Format: april-18-2025-police-report-log.pdf
        # Note: The path structure might change, adjust if necessary
        url_path = f"/files/assets/public/v/2/police-department/public-information-portal/police-report-log/{month}-{day:02d}-{year}-police-report-log.pdf"
        full_url = urljoin(BASE_URL, url_path)
        urls.append(full_url)
        
        current_date += datetime.timedelta(days=1) # Move to the next day
    
    return urls

def download_report(url, output_dir):
    """Download a police report PDF from the given URL."""
    try:
        filename = url.split('/')[-1]
        output_path = os.path.join(output_dir, filename)
        
        # Skip if file already exists
        if os.path.exists(output_path):
            print(f"File {filename} already exists, skipping...")
            return output_path, True # Return True indicating skipped/exists
        
        # Check if the URL actually exists before attempting download
        head_response = requests.head(url, allow_redirects=True)
        if head_response.status_code != 200:
             print(f"URL {url} returned status {head_response.status_code}, skipping...")
             return None, False # Return False indicating not found

        response = requests.get(url, stream=True)
        response.raise_for_status() # Raise for bad status codes (4xx or 5xx)

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Downloaded {filename}")
        return output_path, True # Return True indicating success
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None, False # Return False indicating error

def main(start_date_str, end_date_str):
    """Main function to download police reports for a date range."""
    try:
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format.")
        sys.exit(1)

    if start_date > end_date:
        print("Error: Start date cannot be after end date.")
        sys.exit(1)

    ensure_directory_exists(DATA_DIR)
    
    print(f"Generating report URLs from {start_date_str} to {end_date_str}...")
    urls_to_try = generate_report_urls(start_date, end_date)
    
    print(f"Generated {len(urls_to_try)} potential report URLs")
    
    # Download each report
    successful_downloads = 0
    skipped_existing = 0
    not_found = 0
    errors = 0

    for url in tqdm(urls_to_try, desc="Downloading reports"):
        output_path, success = download_report(url, DATA_DIR)
        if output_path and success:
             # Check if it was a new download or skipped
             filename = url.split('/')[-1]
             full_path = os.path.join(DATA_DIR, filename)
             if not os.path.exists(full_path): # Small race condition possible, but unlikely
                  successful_downloads += 1
             else:
                  # It existed before or was just downloaded
                  # If it was skipped, it was already counted
                  if "skipping" not in f"File {filename} already exists, skipping...": # Check if it was newly downloaded
                       successful_downloads += 1 # Count successful new downloads
                  else:
                       skipped_existing += 1 # Count skipped files
        elif not success and not output_path: # Indicates URL likely not found or download failed
             not_found += 1 # Count URLs that didn't resolve or failed
        # We could differentiate between errors and not found if needed

    total_processed = successful_downloads + skipped_existing + not_found + errors
    print(f"--- Download Summary ---")
    print(f"Processed: {len(urls_to_try)} URLs")
    print(f"Successfully downloaded: {successful_downloads} new reports")
    print(f"Skipped (already exist): {skipped_existing} reports")
    print(f"Not found/Error: {not_found} URLs") # Simplified error/not found count

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Palo Alto Police reports for a given date range.")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD format.")
    
    args = parser.parse_args()
    main(args.start_date, args.end_date)