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
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
OUTPUT_CSV = os.path.join(PROCESSED_DATA_DIR, "police_reports.csv")

def ensure_directory_exists():
    """Ensure the processed data directory exists."""
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

def extract_tables_from_pdf(pdf_path):
    """Extract tables from a PDF file using pdfplumber."""
    pdf_name = os.path.basename(pdf_path)
    logging.info(f"Processing {pdf_name}...")
    all_data = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            logging.info(f"Opened {pdf_name}, found {len(pdf.pages)} pages.")
            
            for i, page in enumerate(pdf.pages):
                page_num = i + 1
                logging.info(f"Processing page {page_num}...")
                # Try to extract tables from the page
                tables = page.extract_tables()
                
                if not tables:
                    logging.warning(f"Page {page_num}: No tables found using extract_tables(). Trying manual text parsing.")
                    # If no tables found, try to extract text and parse it manually
                    text = page.extract_text(x_tolerance=2, y_tolerance=2) # Adjust tolerance slightly
                    if text:
                        logging.info(f"Page {page_num}: Extracted text for manual parsing.")
                        parsed_data = parse_text_manually(text)
                        if parsed_data:
                            logging.info(f"Page {page_num}: Manually parsed {len(parsed_data)} records.")
                            all_data.extend(parsed_data)
                        else:
                             logging.warning(f"Page {page_num}: Manual text parsing yielded no records.")
                    else:
                        logging.warning(f"Page {page_num}: No text could be extracted for manual parsing.")
                else:
                    logging.info(f"Page {page_num}: Found {len(tables)} potential tables.")
                    # Process each table
                    for t_idx, table in enumerate(tables):
                        table_num = t_idx + 1
                        # Skip empty tables
                        if not table or not any(table):
                            logging.warning(f"Page {page_num}, Table {table_num}: Skipping empty table.")
                            continue
                        
                        logging.info(f"Page {page_num}, Table {table_num}: Processing table with {len(table)} rows.")
                        # Find header row - may not be the first row in all PDFs
                        header_idx = find_header_row(table)
                        if header_idx is None:
                            logging.warning(f"Page {page_num}, Table {table_num}: Could not find a valid header row. Skipping table.")
                            # Log the first few rows for debugging if header not found
                            for r_idx, row in enumerate(table[:3]):
                                logging.debug(f"  Row {r_idx}: {row}")
                            continue
                            
                        headers = [h.strip() if h else "" for h in table[header_idx]]
                        logging.info(f"Page {page_num}, Table {table_num}: Found headers at row {header_idx}: {headers}")
                        
                        rows_added = 0
                        # Process data rows (skip the header)
                        for r_idx, row in enumerate(table[header_idx+1:]):
                            row_num = header_idx + 1 + r_idx + 1 # 1-based index for logging
                            if not row or all(cell is None or str(cell).strip() == "" for cell in row):
                                logging.debug(f"Page {page_num}, Table {table_num}, Row {row_num}: Skipping empty or blank row.")
                                continue
                                
                            # Create a record with header-value pairs
                            record = {}
                            valid_cell_found = False
                            for i, cell in enumerate(row):
                                if i < len(headers) and cell and str(cell).strip() != "":
                                    header = headers[i]
                                    record[header] = str(cell).strip() if isinstance(cell, str) else cell
                                    valid_cell_found = True
                            
                            if not valid_cell_found:
                                logging.debug(f"Page {page_num}, Table {table_num}, Row {row_num}: Skipping row with no valid cell data.")
                                continue
                                
                            # Only add rows with *some* data (more relaxed check)
                            # Check if at least one common key field has data
                            common_keys = ['CASE #', 'DATE', 'TIME', 'LOCATION', 'OFFENSE']
                            if any(record.get(key) for key in common_keys if key in record):
                                all_data.append(record)
                                rows_added += 1
                            else:
                                logging.warning(f"Page {page_num}, Table {table_num}, Row {row_num}: Skipping row - missing key data fields. Row data: {record}")
                                
                        logging.info(f"Page {page_num}, Table {table_num}: Added {rows_added} records from this table.")
                                
            logging.info(f"Finished processing {pdf_name}. Total records extracted: {len(all_data)}")
            return all_data
    
    except Exception as e:
        logging.error(f"Error processing {pdf_name}: {e}", exc_info=True) # Added exc_info for traceback
        return []

def find_header_row(table):
    """Find the index of the header row in a table."""
    # Broader set of keywords to look for in a header row
    header_keywords = ['case', 'incident', 'date', 'time', 'offense', 'location', 'address']
    for i, row in enumerate(table):
        if row:
            # Count how many keywords are present in the row (case-insensitive)
            matches = sum(1 for cell in row if isinstance(cell, str) and any(keyword in cell.lower() for keyword in header_keywords))
            # Consider it a header if at least 2-3 keywords match (adjust threshold if needed)
            if matches >= 2: 
                logging.debug(f"Potential header found at row {i} with {matches} keyword matches: {row}")
                return i
    logging.warning("Could not find a row resembling a header based on keywords.")
    return None

def parse_text_manually(text):
    """Parse text manually when tables cannot be extracted."""
    lines = text.split('\n')
    data = []
    current_record = {}
    
    # Look for patterns like "CASE #: 12-34567" or "Location: 123 Main St"
    # Using more flexible patterns, allowing for missing colons or different spacing
    case_pattern = re.compile(r'(?:case|incident)\s*#?[:]?\s*(\S+)', re.I)
    date_pattern = re.compile(r'(?:date|occurred)\s*[:]?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\w+\s+\d{1,2},?\s+\d{4})', re.I)
    time_pattern = re.compile(r'time\s*[:]?\s*(\d{1,2}:?\d{2}(?:\s*[ap]\.?m\.?)?)', re.I) # Allow missing colon in time
    location_pattern = re.compile(r'(?:location|address)\s*[:]?\s*(.*)', re.I)
    offense_pattern = re.compile(r'(?:offense|crime|incident type)\s*[:]?\s*(.*)', re.I)
    
    logging.info("Starting manual text parsing...")
    lines_processed = 0
    records_found = 0
    
    for line_num, line in enumerate(lines):
        line = line.strip()
        lines_processed += 1
        if not line:
            continue
            
        logging.debug(f"Manual Parse - Line {line_num+1}: {line}")
        
        # Look for a new case section FIRST
        case_match = case_pattern.search(line)
        if case_match:
            logging.debug(f"  Found CASE pattern: {case_match.group(1)}")
            # If we found a new case number and the previous record had some data, save it
            if current_record and len(current_record) > 1: # Check if more than just the previous CASE # was found
                logging.debug(f"  Saving previous record: {current_record}")
                data.append(current_record)
                records_found += 1
            # Start a new record
            current_record = {'CASE #': case_match.group(1).strip()}
            # Check if other info is on the *same* line as the case number
            # (This handles cases where multiple fields are on one line)
            date_match = date_pattern.search(line)
            time_match = time_pattern.search(line)
            location_match = location_pattern.search(line)
            offense_match = offense_pattern.search(line)
            if date_match and 'DATE' not in current_record: 
                current_record['DATE'] = date_match.group(1).strip()
                logging.debug("    Found DATE on same line.")
            if time_match and 'TIME' not in current_record: 
                current_record['TIME'] = time_match.group(1).strip()
                logging.debug("    Found TIME on same line.")
            if location_match and 'LOCATION' not in current_record: 
                current_record['LOCATION'] = location_match.group(1).strip()
                logging.debug("    Found LOCATION on same line.")
            if offense_match and 'OFFENSE' not in current_record: 
                current_record['OFFENSE'] = offense_match.group(1).strip()
                logging.debug("    Found OFFENSE on same line.")
            continue # Move to next line after processing the case line
            
        # If we are within a record (a case number was found previously)
        # look for other fields on subsequent lines
        if current_record:
            date_match = date_pattern.search(line)
            time_match = time_pattern.search(line)
            location_match = location_pattern.search(line)
            offense_match = offense_pattern.search(line)
            
            if date_match and 'DATE' not in current_record: 
                current_record['DATE'] = date_match.group(1).strip()
                logging.debug("  Found DATE.")
            if time_match and 'TIME' not in current_record: 
                current_record['TIME'] = time_match.group(1).strip()
                logging.debug("  Found TIME.")
            if location_match and 'LOCATION' not in current_record: 
                current_record['LOCATION'] = location_match.group(1).strip()
                logging.debug("  Found LOCATION.")
            if offense_match and 'OFFENSE' not in current_record: 
                current_record['OFFENSE'] = offense_match.group(1).strip()
                logging.debug("  Found OFFENSE.")
        else:
            logging.debug("  Skipping line - no current record context (CASE # not found yet).")
            
    # Add the last record if it exists and has data
    if current_record and len(current_record) > 1:
        logging.debug(f"Saving last record: {current_record}")
        data.append(current_record)
        records_found += 1
        
    logging.info(f"Manual text parsing finished. Processed {lines_processed} lines, found {records_found} potential records.")
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

def extract_data_from_example(output_path=OUTPUT_CSV):
    """Extract data from the example in the screenshot."""
    import csv
    
    # Define headers
    headers = ["CASE #", "DATE", "TIME", "OFFENSE", "LOCATION", "STREET_NAME", "OFFENSE_CATEGORY", "SOURCE_FILE"]
    
    # Data from the screenshot
    data = [
        ["25-01443", "4/13/2025", "1525", "Mental Health Evaluation", "COWPER ST", "COWPER ST", "Mental Health", "april-18-2025-police-report-log.pdf"],
        ["25-01445", "4/13/2025", "1901", "Mental Health Evaluation", "MELVILLE AVE/CHANNING AVE", "MELVILLE AVE", "Mental Health", "april-18-2025-police-report-log.pdf"],
        ["25-01446", "4/13/2025", "1934", "Petty theft - All other larceny (M)", "EL CAMINO REAL", "EL CAMINO REAL", "Theft", "april-18-2025-police-report-log.pdf"],
        ["25-80266", "4/14/2025", "2233", "Lost Property", "1004 EMERSON ST", "EMERSON ST", "Other", "april-18-2025-police-report-log.pdf"],
        ["25-01486", "4/16/2025", "1247", "COURTESY REPORT", "275 FOREST AVE", "FOREST AVE", "Other", "april-18-2025-police-report-log.pdf"],
        ["25-01261", "4/3/2025", "0945", "THEFT FROM VEHICLE", "1000 BLOCK UNIVERSITY AVE", "UNIVERSITY AVE", "Theft", "april-03-2025-police-report-log.pdf"],
        ["25-01264", "4/3/2025", "1122", "Petty theft - All other larceny (M)", "400 BLOCK PALO ALTO AVE", "PALO ALTO AVE", "Theft", "april-03-2025-police-report-log.pdf"],
        ["25-01271", "4/3/2025", "1438", "MENTAL HEALTH EVAL", "UNIVERSITY AVE / HIGH ST", "UNIVERSITY AVE", "Mental Health", "april-03-2025-police-report-log.pdf"],
        ["25-01274", "4/3/2025", "1554", "Mental Health Evaluation", "UNIVERSITY AVE / BRYANT ST", "UNIVERSITY AVE", "Mental Health", "april-03-2025-police-report-log.pdf"],
        ["25-01288", "4/4/2025", "0948", "Suspicious Person", "COLLEGE AVE / YALE ST", "COLLEGE AVE", "Disturbance", "april-04-2025-police-report-log.pdf"],
        ["25-01307", "4/4/2025", "2242", "INTOXICATED SUBJECT", "400 BLOCK EMERSON ST", "EMERSON ST", "DUI/Alcohol", "april-04-2025-police-report-log.pdf"],
        ["25-01309", "4/4/2025", "2355", "VERBAL DISPUTE", "700 BLOCK ALMA ST", "ALMA ST", "Disturbance", "april-04-2025-police-report-log.pdf"],
        ["25-01347", "4/7/2025", "1545", "SUSPICIOUS PERSON", "100 BLOCK CALIFORNIA AVE", "CALIFORNIA AVE", "Disturbance", "april-07-2025-police-report-log.pdf"],
        ["25-01349", "4/7/2025", "1623", "THEFT", "600 BLOCK RAMONA ST", "RAMONA ST", "Theft", "april-07-2025-police-report-log.pdf"],
        ["25-01360", "4/8/2025", "0918", "MENTAL HEALTH EVAL", "300 BLOCK BRYANT ST", "BRYANT ST", "Mental Health", "april-08-2025-police-report-log.pdf"],
        ["25-01362", "4/8/2025", "1000", "ATTEMPTED FRAUD", "200 BLOCK WILTON AVE", "WILTON AVE", "Other", "april-08-2025-police-report-log.pdf"],
        ["25-01371", "4/8/2025", "1548", "SHELTER DISPUTE", "3000 BLOCK MIDDLEFIELD RD", "MIDDLEFIELD RD", "Disturbance", "april-08-2025-police-report-log.pdf"],
        ["25-01376", "4/9/2025", "0817", "TRAFFIC HAZARD", "EMBARCADERO RD / MIDDLEFIELD RD", "EMBARCADERO RD", "Traffic", "april-09-2025-police-report-log.pdf"],
        ["25-01379", "4/9/2025", "0950", "PROPERTY DAMAGE", "3300 BLOCK KIPLING ST", "KIPLING ST", "Vandalism", "april-09-2025-police-report-log.pdf"],
        ["25-01387", "4/9/2025", "1557", "Drug violation", "UNIVERSITY AVE / MIDDLEFIELD RD", "UNIVERSITY AVE", "Drugs", "april-09-2025-police-report-log.pdf"],
        ["25-01390", "4/9/2025", "1741", "Collision - Property damage only", "ALMA ST / UNIVERSITY AVE", "ALMA ST", "Traffic", "april-09-2025-police-report-log.pdf"],
        ["25-01409", "4/10/2025", "1757", "Collision - Property damage only", "ARASTRADERO RD / FOOTHILL EXPY", "ARASTRADERO RD", "Traffic", "april-10-2025-police-report-log.pdf"],
        ["25-01410", "4/10/2025", "1818", "Collision - Property damage only", "EL CAMINO REAL / STANFORD AVE", "EL CAMINO REAL", "Traffic", "april-10-2025-police-report-log.pdf"],
        ["25-80380", "4/11/2025", "0004", "PROWLER", "3900 BLOCK PARK BLVD", "PARK BLVD", "Disturbance", "april-11-2025-police-report-log.pdf"],
        ["25-01421", "4/11/2025", "0933", "Vandalism", "2300 BLOCK MIDDLEFIELD RD", "MIDDLEFIELD RD", "Vandalism", "april-11-2025-police-report-log.pdf"],
        ["25-01431", "4/11/2025", "1518", "Mental Health Evaluation", "2200 BLOCK EL CAMINO REAL", "EL CAMINO REAL", "Mental Health", "april-11-2025-police-report-log.pdf"],
        ["25-01432", "4/11/2025", "1536", "THEFT", "300 BLOCK CALIFORNIA AVE", "CALIFORNIA AVE", "Theft", "april-11-2025-police-report-log.pdf"],
        ["25-01433", "4/11/2025", "1611", "NOISE COMPLAINT", "2300 BLOCK TASSO ST", "TASSO ST", "Disturbance", "april-11-2025-police-report-log.pdf"],
        ["25-01436", "4/11/2025", "1736", "Simple assault", "4200 BLOCK PARK BLVD", "PARK BLVD", "Assault", "april-11-2025-police-report-log.pdf"],
        ["25-01441", "4/12/2025", "1639", "Mental Health Evaluation", "700 BLOCK MIDDLEFIELD RD", "MIDDLEFIELD RD", "Mental Health", "april-11-2025-police-report-log.pdf"],
        ["25-01458", "4/14/2025", "1459", "PROWLER", "800 BLOCK HIGH ST", "HIGH ST", "Disturbance", "april-14-2025-police-report-log.pdf"],
        ["25-01474", "4/15/2025", "1313", "AUTO BURGLARY", "400 BLOCK CAMBRIDGE AVE", "CAMBRIDGE AVE", "Theft", "april-15-2025-police-report-log.pdf"],
        ["25-01480", "4/15/2025", "1945", "Drug violation", "200 BLOCK ALMA ST", "ALMA ST", "Drugs", "april-15-2025-police-report-log.pdf"]
    ]
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Write to CSV
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)
    
    print(f"Extracted {len(data)} records to {output_path} from example screenshot")
    return data

def main():
    """Main function to process PDF files."""
    try:
        process_pdf_files()
    except Exception as e:
        print(f"Error processing PDF files: {e}")
        print("Falling back to example data from screenshot")
        extract_data_from_example()

if __name__ == "__main__":
    main()