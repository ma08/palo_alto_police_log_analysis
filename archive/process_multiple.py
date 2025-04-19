#!/usr/bin/env python3
"""
process_multiple.py - Process multiple PDF files with vision extraction.
"""

import os
import json
import glob
from tqdm import tqdm
from vision_extract import (
    ensure_directories_exist, 
    convert_pdf_to_images, 
    extract_with_bedrock_claude, 
    normalize_records,
    save_results,
    analyze_with_llm
)

def process_pdfs(num_files=5):
    """Process multiple PDF files with vision extraction."""
    # Setup directories
    ensure_directories_exist()
    
    # Get PDF files
    pdf_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "raw")
    pdf_files = sorted(glob.glob(os.path.join(pdf_dir, "*.pdf")))
    
    if not pdf_files:
        print(f"No PDF files found in {pdf_dir}")
        return
    
    # Limit to specified number
    pdf_files = pdf_files[:num_files]
    
    print(f"Processing {len(pdf_files)} PDF files: {[os.path.basename(f) for f in pdf_files]}")
    
    all_records = []
    
    # Process each PDF
    for pdf_file in tqdm(pdf_files, desc="Processing PDFs"):
        pdf_filename = os.path.basename(pdf_file)
        
        print(f"\nProcessing {pdf_filename}...")
        
        # Convert PDF to images
        image_paths = convert_pdf_to_images(pdf_file)
        
        if not image_paths:
            print(f"No images generated from {pdf_filename}")
            continue
        
        pdf_records = []
        
        # Process each page
        for i, image_path in enumerate(tqdm(image_paths, desc=f"Pages from {pdf_filename}", leave=False)):
            # Extract data from image
            records = extract_with_bedrock_claude(image_path)
            
            if records:
                # Add source file information
                for record in records:
                    record['source_file'] = pdf_filename
                
                pdf_records.extend(records)
                print(f"Extracted {len(records)} records from page {i+1}")
            else:
                print(f"No records from page {i+1}")
        
        all_records.extend(pdf_records)
        print(f"Total {len(pdf_records)} records from {pdf_filename}")
    
    # Normalize and save records
    if all_records:
        print(f"\nProcessing {len(all_records)} total records...")
        normalized_records = normalize_records(all_records)
        
        # Get some stats
        locations = {}
        for record in normalized_records:
            street = record.get('STREET_NAME')
            if street:
                locations[street] = locations.get(street, 0) + 1
        
        top_locations = sorted(locations.items(), key=lambda x: x[1], reverse=True)
        print("\nTop 10 locations:")
        for street, count in top_locations[:10]:
            print(f"- {street}: {count} incidents")
        
        # Save results
        save_results(normalized_records)
        print(f"Saved {len(normalized_records)} records to CSV")
        
        # Run LLM analysis if we have enough records
        if len(normalized_records) >= 10:
            print("\nRunning LLM analysis...")
            try:
                analysis = analyze_with_llm(normalized_records)
                print("LLM analysis complete.")
                
                # Print recommendations
                if 'recommendations' in analysis:
                    print("\nRecommendations for house hunting:")
                    for rec in analysis['recommendations']:
                        print(f"- {rec}")
            except Exception as e:
                print(f"Error in LLM analysis: {e}")
    else:
        print("No records were extracted")

if __name__ == "__main__":
    process_pdfs(5)  # Process up to 5 PDFs