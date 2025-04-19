#!/usr/bin/env python3
"""
extract_data.py - Extracts structured data from police report PDFs.
"""

import os
import re
import csv
import glob
import pdfplumber
import pandas as pd
from tqdm import tqdm

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
OUTPUT_CSV = os.path.join(PROCESSED_DATA_DIR, "police_reports.csv")

def ensure_directory_exists():
    """Ensure the processed data directory exists."""
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

def extract_tables_from_pdf(pdf_path):
    """Extract tables from a PDF file using pdfplumber."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_data = []
            
            for page in pdf.pages:
                # Try to extract tables from the page
                tables = page.extract_tables()
                
                if not tables:
                    # If no tables found, try to extract text and parse it manually
                    text = page.extract_text()
                    if text:
                        parsed_data = parse_text_manually(text)
                        if parsed_data:
                            all_data.extend(parsed_data)
                else:
                    # Process each table
                    for table in tables:
                        # Skip empty tables
                        if not table or not any(table):
                            continue
                        
                        # Find header row - may not be the first row in all PDFs
                        header_idx = find_header_row(table)
                        if header_idx is None:
                            continue
                            
                        headers = [h.strip() if h else "" for h in table[header_idx]]
                        
                        # Process data rows (skip the header)
                        for row in table[header_idx+1:]:
                            if not row or all(cell is None or cell.strip() == "" for cell in row):
                                continue
                                
                            # Create a record with header-value pairs
                            record = {}
                            for i, cell in enumerate(row):
                                if i < len(headers) and cell:
                                    header = headers[i]
                                    record[header] = cell.strip() if isinstance(cell, str) else cell
                            
                            # Only add rows with case numbers (likely valid data rows)
                            case_key = next((h for h in headers if 'case' in h.lower()), None)
                            if case_key and record.get(case_key):
                                all_data.append(record)
                                
            return all_data
    
    except Exception as e:
        print(f"Error processing {os.path.basename(pdf_path)}: {e}")
        return []

def find_header_row(table):
    """Find the index of the header row in a table."""
    for i, row in enumerate(table):
        if row and any(isinstance(cell, str) and ('case' in cell.lower() or 'offense' in cell.lower() or 'location' in cell.lower()) for cell in row):
            return i
    return None

def parse_text_manually(text):
    """Parse text manually when tables cannot be extracted."""
    lines = text.split('\n')
    data = []
    current_record = {}
    
    # Look for patterns like "CASE #: 12-34567" or "Location: 123 Main St"
    case_pattern = re.compile(r'case\s*#?:\s*(\S+)', re.I)
    date_pattern = re.compile(r'date\s*:\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\w+\s+\d{1,2},?\s+\d{4})', re.I)
    time_pattern = re.compile(r'time\s*:\s*(\d{1,2}:\d{2}(?:\s*[ap]\.?m\.?)?)', re.I)
    location_pattern = re.compile(r'location\s*:\s*(.*)', re.I)
    offense_pattern = re.compile(r'offense\s*:\s*(.*)', re.I)
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Look for a new case section
        case_match = case_pattern.search(line)
        if case_match:
            if current_record:
                data.append(current_record)
                current_record = {}
            current_record['CASE #'] = case_match.group(1)
            continue
            
        # Look for other fields
        date_match = date_pattern.search(line)
        time_match = time_pattern.search(line)
        location_match = location_pattern.search(line)
        offense_match = offense_pattern.search(line)
        
        if date_match:
            current_record['DATE'] = date_match.group(1)
        if time_match:
            current_record['TIME'] = time_match.group(1)
        if location_match:
            current_record['LOCATION'] = location_match.group(1)
        if offense_match:
            current_record['OFFENSE'] = offense_match.group(1)
    
    # Add the last record if it exists
    if current_record:
        data.append(current_record)
        
    return data

def extract_street_name(location):
    """Extract the street name from a location string."""
    if not location or not isinstance(location, str):
        return None
        
    # Common street suffixes
    suffixes = ['ST', 'AVE', 'BLVD', 'RD', 'DR', 'CT', 'LN', 'WAY', 'PL', 'CIR']
    
    # Try to find a street suffix
    for suffix in suffixes:
        # Look for the suffix followed by a space or end of string
        pattern = rf'\b{suffix}\b'
        match = re.search(pattern, location.upper())
        if match:
            # Find the start of the street name (likely after a number)
            address_parts = location.split()
            for i, part in enumerate(address_parts):
                if part.upper() == suffix:
                    # Look backwards for the street name
                    street_parts = []
                    j = i - 1
                    # Skip the street number
                    while j >= 0 and not re.match(r'^\d+$', address_parts[j]):
                        street_parts.insert(0, address_parts[j])
                        j -= 1
                    if street_parts:
                        return ' '.join(street_parts + [suffix])
    
    # If no street suffix found, try to parse based on common patterns
    # For intersections like "ALMA ST & HAMILTON AVE"
    intersection_match = re.search(r'([A-Za-z\s]+)(?:\s+&\s+|\s+and\s+)([A-Za-z\s]+)', location)
    if intersection_match:
        return intersection_match.group(1).strip()
        
    # For block addresses like "600 block of FOREST AVE"
    block_match = re.search(r'block\s+of\s+([A-Za-z\s]+)', location)
    if block_match:
        return block_match.group(1).strip()
    
    # Default to returning the whole location if we can't find a specific street
    return location

def normalize_categories(offense):
    """Normalize offense categories."""
    if not offense or not isinstance(offense, str):
        return "Unknown"
    
    offense = offense.lower()
    
    if any(term in offense for term in ['theft', 'burglary', 'robbery', 'shoplifting', 'stolen']):
        return "Theft"
    elif any(term in offense for term in ['assault', 'battery', 'fight', 'violence']):
        return "Assault"
    elif any(term in offense for term in ['drug', 'narcotic', 'possession']):
        return "Drugs"
    elif any(term in offense for term in ['dui', 'driving under', 'alcohol', 'intoxicated']):
        return "DUI/Alcohol"
    elif any(term in offense for term in ['vandalism', 'graffiti', 'property damage']):
        return "Vandalism"
    elif any(term in offense for term in ['traffic', 'collision', 'accident', 'vehicle']):
        return "Traffic"
    elif any(term in offense for term in ['mental', 'welfare', 'health']):
        return "Mental Health"
    elif any(term in offense for term in ['trespass', 'suspicious', 'disturb']):
        return "Disturbance"
    else:
        return "Other"

def process_pdf_files():
    """Process all PDF files in the raw data directory."""
    ensure_directory_exists()
    
    # Get all PDF files
    pdf_files = glob.glob(os.path.join(RAW_DATA_DIR, "*.pdf"))
    
    if not pdf_files:
        print(f"No PDF files found in {RAW_DATA_DIR}")
        return
    
    all_records = []
    
    for pdf_file in tqdm(pdf_files, desc="Processing PDFs"):
        pdf_date_match = re.search(r'(\w+)-(\d{1,2})-(\d{4})', os.path.basename(pdf_file))
        pdf_date = None
        if pdf_date_match:
            month, day, year = pdf_date_match.groups()
            pdf_date = f"{month} {day}, {year}"
            
        records = extract_tables_from_pdf(pdf_file)
        
        # Add source file and date information to each record
        for record in records:
            record['SOURCE_FILE'] = os.path.basename(pdf_file)
            if pdf_date and 'DATE' not in record:
                record['DATE'] = pdf_date
            
            # Extract street name from location
            if 'LOCATION' in record:
                record['STREET_NAME'] = extract_street_name(record['LOCATION'])
            
            # Normalize offense categories
            if 'OFFENSE' in record:
                record['OFFENSE_CATEGORY'] = normalize_categories(record['OFFENSE'])
        
        all_records.extend(records)
    
    # Convert to DataFrame
    if all_records:
        df = pd.DataFrame(all_records)
        
        # Write to CSV
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"Extracted {len(df)} records to {OUTPUT_CSV}")
    else:
        print("No records were extracted from the PDFs")

def main():
    """Main function to process PDF files."""
    process_pdf_files()

if __name__ == "__main__":
    main()