#!/usr/bin/env python3
"""
test_single_file.py - Test the vision extraction on a single PDF file.
"""

import os
import json
from vision_extract import (
    ensure_directories_exist, 
    convert_pdf_to_images, 
    extract_with_bedrock_claude, 
    normalize_records,
    save_results
)

def process_single_pdf(pdf_file="april-18-2025-police-report-log.pdf", page_limit=1):
    """Process a single PDF file with vision extraction."""
    # Setup directories
    ensure_directories_exist()
    
    # Path to the PDF
    pdf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "raw", pdf_file)
    
    print(f"Testing vision extraction on: {pdf_path}")
    
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found at {pdf_path}")
        return
    
    # Convert PDF to images
    image_paths = convert_pdf_to_images(pdf_path)
    
    if not image_paths:
        print("No images were generated from the PDF")
        return
    
    # Limit pages if specified
    if page_limit > 0:
        image_paths = image_paths[:page_limit]
    
    print(f"Processing {len(image_paths)} pages from the PDF")
    
    all_records = []
    
    # Process pages
    for i, image_path in enumerate(image_paths):
        print(f"Extracting from page {i+1}/{len(image_paths)}...")
        
        # Extract data from the image
        records = extract_with_bedrock_claude(image_path)
        
        if records:
            print(f"Successfully extracted {len(records)} records from page {i+1}")
            if len(records) > 0:
                print(json.dumps(records, indent=2))
                
            # Add source file information
            for record in records:
                record['source_file'] = pdf_file
                
            all_records.extend(records)
        else:
            print(f"No records extracted from page {i+1}")
    
    if all_records:
        print(f"Total records extracted: {len(all_records)}")
        
        # Print extracted data
        print("Extracted records:")
        for record in all_records:
            location = record.get('location', 'No location')
            offense = record.get('offense', 'Unknown')
            case = record.get('case_number', 'No case number')
            print(f"- Case {case}: {offense} at {location}")
        
        # Normalize records
        normalized = normalize_records(all_records)
        
        # Save to CSV for testing
        save_results(normalized)
        print(f"Saved {len(normalized)} records to CSV")
    else:
        print("No records were extracted from any page")

if __name__ == "__main__":
    process_single_pdf()