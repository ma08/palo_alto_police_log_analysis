#!/usr/bin/env python3
"""
Palo Alto Police Report Data Extractor

This script downloads police report PDFs from the Palo Alto police department website,
extracts structured data from them, and saves the data in CSV format for analysis.

Usage:
    python pdf_extractor.py

Author: Sourya Kakarla
Date: April 18, 2025
"""

import os
import re
import csv
import requests
import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import pandas as pd
import pdfplumber
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("extraction.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.paloalto.gov/files/assets/public/v/2/police-department/public-information-portal/police-report-log/"
RAW_PDF_DIR = Path("/Users/sourya4/pro/palo_alto_police_report_analysis/data/raw_pdfs")
PROCESSED_DATA_DIR = Path("/Users/sourya4/pro/palo_alto_police_report_analysis/data/processed_data")
OUTPUT_CSV = PROCESSED_DATA_DIR / "police_reports_data.csv"

# Ensure directories exist
RAW_PDF_DIR.mkdir(exist_ok=True, parents=True)
PROCESSED_DATA_DIR.mkdir(exist_ok=True, parents=True)

class PaloAltoPDFExtractor:
    """Class for downloading and extracting data from Palo Alto police report PDFs."""
    
    def __init__(self):
        """Initialize the extractor with default values."""
        self.reports_data = []
        
    def generate_date_range(self, end_date=None, days=30):
        """
        Generate a list of dates for the last specified number of days.
        
        Args:
            end_date (datetime.date): The end date (defaults to today)
            days (int): Number of days to go back
            
        Returns:
            list: List of datetime.date objects
        """
        if end_date is None:
            end_date = datetime.date.today()
        elif isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
            
        date_list = []
        for i in range(days):
            date_list.append(end_date - datetime.timedelta(days=i))
            
        return date_list
    
    def generate_url(self, date):
        """
        Generate URL for a police report PDF based on date.
        
        Args:
            date (datetime.date): The date of the report
            
        Returns:
            str: URL of the PDF
        """
        formatted_date = date.strftime("%B-%d-%Y").lower()
        url = urljoin(BASE_URL, f"{formatted_date}-police-report-log.pdf")
        return url
    
    def download_pdf(self, url, output_path):
        """
        Download a PDF from URL and save it to the specified path.
        
        Args:
            url (str): URL of the PDF
            output_path (Path): Path to save the PDF
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()  # Raise error for bad status codes
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Successfully downloaded: {url}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            return False
    
    def download_pdfs_for_date_range(self, date_range):
        """
        Download PDFs for a list of dates.
        
        Args:
            date_range (list): List of datetime.date objects
            
        Returns:
            list: List of paths to successfully downloaded PDFs
        """
        successful_downloads = []
        
        # Create a ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=5) as executor:
            download_tasks = []
            
            for date in date_range:
                url = self.generate_url(date)
                output_path = RAW_PDF_DIR / f"{date.strftime('%Y-%m-%d')}_police_report.pdf"
                
                # If file already exists, skip download
                if output_path.exists():
                    logger.info(f"File already exists: {output_path}")
                    successful_downloads.append(output_path)
                    continue
                
                # Submit download task to executor
                task = executor.submit(self.download_pdf, url, output_path)
                download_tasks.append((task, output_path))
            
            # Process completed tasks
            for task, output_path in download_tasks:
                if task.result():
                    successful_downloads.append(output_path)
        
        return successful_downloads
    
    def extract_text_from_pdf(self, pdf_path):
        """
        Extract text from a PDF file.
        
        Args:
            pdf_path (Path): Path to the PDF file
            
        Returns:
            str: Extracted text
        """
        try:
            text = ""
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text += page.extract_text() or ""
            return text
        except Exception as e:
            logger.error(f"Error extracting text from {pdf_path}: {e}")
            return ""
    
    def parse_report_data(self, text, report_date):
        """
        Parse structured data from report text.
        
        Args:
            text (str): Extracted text from the PDF
            report_date (datetime.date): Date of the report
            
        Returns:
            list: List of dictionaries containing structured report data
        """
        reports = []
        
        # Try to split by report entries
        # This pattern may need adjusting based on the actual format
        report_pattern = r"(\d{8})\s+(\d{1,2}/\d{1,2}/\d{2,4})\s+(\d{1,2}:\d{2})\s+(.*?)\s{2,}(.*?)(?=\d{8}|\Z)"
        
        matches = re.finditer(report_pattern, text, re.DOTALL)
        
        for match in matches:
            try:
                case_number = match.group(1)
                date_str = match.group(2)
                time_str = match.group(3)
                offense = match.group(4).strip()
                location_details = match.group(5).strip()
                
                # Extract street name from location
                street_name = self.extract_street_name(location_details)
                
                # Categorize offense
                offense_category = self.categorize_offense(offense)
                
                reports.append({
                    "case_number": case_number,
                    "date": date_str,
                    "time": time_str,
                    "offense": offense,
                    "offense_category": offense_category,
                    "location_full": location_details,
                    "street_name": street_name,
                    "report_date": report_date.strftime("%Y-%m-%d")
                })
                
            except Exception as e:
                logger.warning(f"Error parsing report entry: {e}")
                continue
        
        # If the standard pattern fails, try an alternative approach
        if not reports:
            logger.warning(f"Standard pattern failed for report date {report_date}, trying alternative parsing")
            reports = self.alternative_parsing(text, report_date)
        
        return reports
    
    def extract_street_name(self, location_text):
        """
        Extract street name from location text.
        
        Args:
            location_text (str): Full location text
            
        Returns:
            str: Extracted street name
        """
        # Common Palo Alto street patterns
        street_pattern = r'(\b\d+\s+\w+\s+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Place|Pl|Court|Ct)\b|\b\w+\s+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Place|Pl|Court|Ct)\b)'
        
        match = re.search(street_pattern, location_text, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # If no match found, try to find any capitalized words that might be street names
        words = location_text.split()
        for i, word in enumerate(words):
            if word[0].isupper() and i < len(words) - 1 and not words[i+1].startswith("("):
                return f"{word} {words[i+1]}"
        
        return "Unknown"
    
    def categorize_offense(self, offense_text):
        """
        Categorize offense based on text.
        
        Args:
            offense_text (str): Description of the offense
            
        Returns:
            str: Category of offense
        """
        offense_text = offense_text.lower()
        
        categories = {
            "theft": ["theft", "burglary", "rob", "stole", "shoplifting", "larceny"],
            "vandalism": ["vandalism", "graffiti", "damage to property"],
            "assault": ["assault", "battery", "fight", "attack"],
            "drugs": ["drug", "narcotics", "substance", "marijuana"],
            "traffic": ["traffic", "dui", "driving", "vehicle", "collision", "accident"],
            "domestic": ["domestic", "family", "household"],
            "fraud": ["fraud", "identity theft", "scam", "counterfeit"],
            "trespass": ["trespass", "prowl", "loiter"],
            "noise": ["noise", "disturbance", "loud"],
            "weapons": ["weapon", "firearm", "gun", "knife"]
        }
        
        for category, keywords in categories.items():
            if any(keyword in offense_text for keyword in keywords):
                return category
                
        return "other"
    
    def alternative_parsing(self, text, report_date):
        """
        Alternative parsing method when standard method fails.
        
        Args:
            text (str): Extracted text from the PDF
            report_date (datetime.date): Date of the report
            
        Returns:
            list: List of dictionaries containing structured report data
        """
        reports = []
        
        # Split by lines and try to identify report entries
        lines = text.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Try to identify lines that start with case numbers (8 digits)
            if re.match(r'^\d{8}', line):
                try:
                    # Extract what data we can from this line
                    parts = re.split(r'\s{2,}', line)
                    
                    if len(parts) >= 3:
                        case_number = parts[0].strip()
                        date_time = parts[1].strip()
                        offense = parts[2].strip()
                        
                        # Location might be on the next line
                        location = ""
                        if i + 1 < len(lines) and not re.match(r'^\d{8}', lines[i+1]):
                            location = lines[i+1].strip()
                            i += 1
                        
                        # Extract date and time if possible
                        date_str = "Unknown"
                        time_str = "Unknown"
                        date_time_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})\s+(\d{1,2}:\d{2})', date_time)
                        if date_time_match:
                            date_str = date_time_match.group(1)
                            time_str = date_time_match.group(2)
                        
                        street_name = self.extract_street_name(location)
                        offense_category = self.categorize_offense(offense)
                        
                        reports.append({
                            "case_number": case_number,
                            "date": date_str,
                            "time": time_str,
                            "offense": offense,
                            "offense_category": offense_category,
                            "location_full": location,
                            "street_name": street_name,
                            "report_date": report_date.strftime("%Y-%m-%d")
                        })
                    
                except Exception as e:
                    logger.warning(f"Error in alternative parsing: {e}")
                    
            i += 1
        
        return reports
    
    def process_pdfs(self, pdf_paths):
        """
        Process a list of PDFs and extract structured data.
        
        Args:
            pdf_paths (list): List of paths to PDF files
            
        Returns:
            list: Combined list of all extracted report data
        """
        all_reports = []
        
        for pdf_path in pdf_paths:
            try:
                # Extract report date from filename
                filename = pdf_path.name
                report_date = datetime.datetime.strptime(filename.split('_')[0], '%Y-%m-%d').date()
                
                logger.info(f"Processing {pdf_path}")
                
                # Extract text from PDF
                text = self.extract_text_from_pdf(pdf_path)
                
                if not text:
                    logger.warning(f"No text extracted from {pdf_path}")
                    continue
                
                # Parse report data
                reports = self.parse_report_data(text, report_date)
                
                logger.info(f"Extracted {len(reports)} reports from {pdf_path}")
                all_reports.extend(reports)
                
            except Exception as e:
                logger.error(f"Error processing {pdf_path}: {e}")
                
        return all_reports
    
    def save_to_csv(self, reports, output_path):
        """
        Save extracted report data to CSV.
        
        Args:
            reports (list): List of report dictionaries
            output_path (Path): Output CSV path
            
        Returns:
            bool: True if successful
        """
        try:
            if not reports:
                logger.warning("No reports to save!")
                return False
            
            # Convert to DataFrame for easier handling
            df = pd.DataFrame(reports)
            
            # Save to CSV
            df.to_csv(output_path, index=False)
            
            logger.info(f"Successfully saved {len(reports)} reports to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return False
    
    def run_extraction(self, end_date=None, days=30):
        """
        Run the full extraction process.
        
        Args:
            end_date (str or datetime.date): End date for the range
            days (int): Number of days to go back
            
        Returns:
            Path: Path to the output CSV file
        """
        logger.info("Starting extraction process")
        
        # Generate date range
        date_range = self.generate_date_range(end_date, days)
        logger.info(f"Generated date range from {date_range[-1]} to {date_range[0]}")
        
        # Download PDFs
        pdf_paths = self.download_pdfs_for_date_range(date_range)
        logger.info(f"Downloaded {len(pdf_paths)} PDFs")
        
        # Process PDFs
        reports = self.process_pdfs(pdf_paths)
        logger.info(f"Processed {len(reports)} total reports")
        
        # Save to CSV
        self.save_to_csv(reports, OUTPUT_CSV)
        
        return OUTPUT_CSV

def main():
    """Main function to run the extraction process."""
    extractor = PaloAltoPDFExtractor()
    
    # Set end date to April 18, 2025 and extract for last 30 days
    end_date = datetime.date(2025, 4, 18)
    output_path = extractor.run_extraction(end_date=end_date, days=30)
    
    print(f"\nExtraction complete! Data saved to: {output_path}")
    print(f"Date range: March 19, 2025 to April 18, 2025")
    print("\nNext steps:")
    print("1. Analyze the data with pandas:")
    print("   - Crime frequency by location")
    print("   - Offense categories by time of day")
    print("   - Trends over the 30-day period")
    print("2. Create visualizations using matplotlib/seaborn")
    print("3. Generate a geospatial analysis with folium if coordinates are available")

if __name__ == "__main__":
    main()