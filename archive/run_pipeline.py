#!/usr/bin/env python3
"""
run_pipeline.py - Run the entire analysis pipeline with vision-based extraction.
"""

import os
import sys
import json
import glob
import time
import subprocess
from tqdm import tqdm

# Import core functions from our modules
from vision_extract_bedrock import (
    ensure_directories_exist,
    convert_pdf_to_images,
    extract_with_bedrock_claude,
    normalize_records,
    analyze_with_llm,
    save_results
)

def setup_directories():
    """Set up all required directories."""
    # Define directories
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    raw_dir = os.path.join(data_dir, "raw")
    processed_dir = os.path.join(data_dir, "processed")
    images_dir = os.path.join(data_dir, "images")
    results_dir = os.path.join(base_dir, "results")
    
    # Create all directories
    for directory in [raw_dir, processed_dir, images_dir, results_dir]:
        os.makedirs(directory, exist_ok=True)
    
    return {
        "base_dir": base_dir,
        "data_dir": data_dir,
        "raw_dir": raw_dir,
        "processed_dir": processed_dir,
        "images_dir": images_dir,
        "results_dir": results_dir
    }

def process_pdf_reports(raw_dir, limit=5):
    """Process PDF reports using vision-based extraction."""
    # Get PDF files
    pdf_files = sorted(glob.glob(os.path.join(raw_dir, "*.pdf")))
    
    if not pdf_files:
        print(f"No PDF files found in {raw_dir}")
        return []
    
    # Limit to specified number
    pdf_files = pdf_files[:limit]
    
    print(f"Processing {len(pdf_files)} PDF files")
    
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
        
        # Process each page (limit to 2 pages per PDF for speed)
        for i, image_path in enumerate(tqdm(image_paths[:2], desc=f"Pages from {pdf_filename}")):
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
    
    print(f"\nExtracted {len(all_records)} total records")
    
    # Return normalized records
    return normalize_records(all_records)

def generate_final_report(records, results_dir):
    """Generate comprehensive final report."""
    if not records:
        print("No records to analyze")
        return
    
    # Get some basic statistics
    locations = {}
    offense_types = {}
    
    for record in records:
        # Count locations
        street = record.get('STREET_NAME')
        if street:
            locations[street] = locations.get(street, 0) + 1
        
        # Count offense types
        offense = record.get('OFFENSE_CATEGORY')
        if offense:
            offense_types[offense] = offense_types.get(offense, 0) + 1
    
    # Get top locations and offense types
    top_locations = sorted(locations.items(), key=lambda x: x[1], reverse=True)
    offense_breakdown = sorted(offense_types.items(), key=lambda x: x[1], reverse=True)
    
    # Calculate percentages for offense types
    total_offenses = sum(offense_types.values())
    offense_percentages = {k: (v / total_offenses) * 100 for k, v in offense_types.items()}
    
    # Create report
    report_path = os.path.join(results_dir, "safety_analysis.md")
    
    with open(report_path, 'w') as f:
        f.write("# Palo Alto Safety Analysis for House Hunting\n\n")
        
        # Overview section
        f.write("## Analysis Overview\n")
        f.write(f"This analysis is based on {len(records)} police incidents extracted from Palo Alto Police Department reports over a 30-day period.\n\n")
        
        # Top locations section
        f.write("## Areas by Incident Frequency\n")
        f.write("### Locations with More Incidents\n")
        for location, count in top_locations[:10]:
            f.write(f"- **{location}**: {count} incidents\n")
        
        f.write("\n### Locations with Fewer Incidents\n")
        for location, count in sorted(top_locations[-10:], key=lambda x: x[1]):
            if count < 2:  # Only show locations with few incidents
                f.write(f"- **{location}**: {count} incident\n")
        
        # Offense types section
        f.write("\n## Types of Incidents\n")
        for offense, count in offense_breakdown:
            percentage = offense_percentages[offense]
            f.write(f"- **{offense}**: {count} incidents ({percentage:.1f}%)\n")
        
        # Safety recommendations section
        f.write("\n## Safety Recommendations for Families\n")
        
        # Derive some recommendations
        safest_areas = [loc for loc, count in sorted(locations.items(), key=lambda x: x[1]) if count < 2]
        concerning_areas = [loc for loc, count in sorted(locations.items(), key=lambda x: x[1], reverse=True) if count > 2]
        
        f.write("\n### Areas with Fewer Safety Concerns\n")
        for area in safest_areas[:5]:
            f.write(f"- **{area}**: Lower incident frequency\n")
        
        f.write("\n### Areas with More Safety Concerns\n")
        for area in concerning_areas[:5]:
            f.write(f"- **{area}**: Higher incident frequency\n")
        
        f.write("\n## Additional Recommendations\n")
        f.write("- Visit neighborhoods at different times of day to get a feel for activity levels\n")
        f.write("- Speak with current residents about their safety experiences\n")
        f.write("- Consider proximity to schools, parks, and community services\n")
        f.write("- Look at street lighting and visibility in potential neighborhoods\n")
        f.write("- Research longer-term crime trends beyond this 30-day snapshot\n")
    
    print(f"Generated safety analysis report: {report_path}")
    return report_path

def run_pipeline():
    """Run the end-to-end analysis pipeline."""
    print("\nPalo Alto Police Report Analysis Pipeline\n")
    
    print("Setting up directories...")
    dirs = setup_directories()
    
    print("Processing PDF reports with vision-based extraction...")
    records = process_pdf_reports(dirs["raw_dir"])
    
    if records:
        # Save the extracted records
        csv_path = os.path.join(dirs["processed_dir"], "police_reports_final.csv")
        df_output = save_results(records)
        
        print("Generating final safety analysis report...")
        report_path = generate_final_report(records, dirs["results_dir"])
        
        # Try to run LLM analysis if we have enough records
        if len(records) >= 10:
            print("\nRunning LLM analysis with Claude...")
            try:
                analysis = analyze_with_llm(records)
                analysis_path = os.path.join(dirs["results_dir"], "llm_analysis.md")
                
                # Create markdown report from LLM analysis
                with open(analysis_path, 'w') as f:
                    f.write("# Claude's Analysis of Palo Alto Safety\n\n")
                    
                    # Write each section
                    sections = [
                        ("Safest Areas", "safest_areas"),
                        ("Areas with Safety Concerns", "concerning_areas"),
                        ("Temporal Patterns", "temporal_patterns"),
                        ("Crime Patterns", "crime_patterns"),
                        ("Recommendations for Families", "recommendations")
                    ]
                    
                    for title, key in sections:
                        f.write(f"## {title}\n")
                        for item in analysis.get(key, []):
                            f.write(f"- {item}\n")
                        f.write("\n")
                
                print(f"Generated LLM analysis report: {analysis_path}")
            except Exception as e:
                print(f"Error in LLM analysis: {e}")
    else:
        print("No records were extracted. Pipeline cannot continue.")
        return
    
    # Open results folder
    try:
        results_dir = dirs["results_dir"]
        if sys.platform == 'darwin':  # macOS
            subprocess.run(['open', results_dir])
        elif sys.platform == 'win32':  # Windows
            subprocess.run(['explorer', results_dir])
        else:  # Linux
            subprocess.run(['xdg-open', results_dir])
        print(f"\nResults folder opened: {results_dir}")
    except Exception as e:
        print(f"\nResults saved to: {results_dir}")
    
    print("\nAnalysis pipeline completed successfully!")
    print("\nTo view the safety report, open:")
    print(f"{os.path.join(dirs['results_dir'], 'safety_analysis.md')}")

if __name__ == "__main__":
    run_pipeline()