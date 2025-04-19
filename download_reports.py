#!/usr/bin/env python3
"""
download_reports.py - Downloads police reports from Palo Alto Police Department website.
"""

import os
import datetime
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm

BASE_URL = "https://www.paloalto.gov"
REPORT_LOG_URL = f"{BASE_URL}/departments/police/public-information-portal/police-report-log"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "raw")

def ensure_directory_exists():
    """Ensure the data directory exists."""
    os.makedirs(DATA_DIR, exist_ok=True)

def get_report_links():
    """Fetch the webpage and extract links to the last 30 days of police reports."""
    try:
        response = requests.get(REPORT_LOG_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Find all PDF links that match the pattern of police reports
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if '-Police-Report-Log.pdf' in href:
                links.append(urljoin(BASE_URL, href))
        
        return links
    except requests.RequestException as e:
        print(f"Error fetching report links: {e}")
        return []

def generate_report_urls(days=30):
    """Generate URLs for the last 30 days of police reports."""
    urls = []
    today = datetime.datetime.now()
    
    for i in range(days):
        date = today - datetime.timedelta(days=i)
        month = date.strftime("%B").lower()
        day = date.day
        year = date.year
        
        # Format: april-18-2025-police-report-log.pdf
        url = f"{BASE_URL}/files/assets/public/v/2/police-department/public-information-portal/police-report-log/{month}-{day:02d}-{year}-police-report-log.pdf"
        urls.append(url)
    
    return urls

def download_report(url, output_dir=DATA_DIR):
    """Download a police report PDF from the given URL."""
    try:
        filename = url.split('/')[-1]
        output_path = os.path.join(output_dir, filename)
        
        # Skip if file already exists
        if os.path.exists(output_path):
            print(f"File {filename} already exists, skipping...")
            return output_path
        
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Downloaded {filename}")
        return output_path
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None

def main():
    """Main function to download police reports."""
    ensure_directory_exists()
    
    # Try getting links from the webpage first
    print("Fetching police report links from website...")
    links = get_report_links()
    
    # If web scraping doesn't work, generate URLs directly
    if not links:
        print("Generating report URLs for the last 30 days...")
        links = generate_report_urls(30)
    
    print(f"Found {len(links)} report URLs")
    
    # Download each report
    successful_downloads = 0
    for url in tqdm(links, desc="Downloading reports"):
        if download_report(url):
            successful_downloads += 1
    
    print(f"Downloaded {successful_downloads} out of {len(links)} reports")

if __name__ == "__main__":
    main()