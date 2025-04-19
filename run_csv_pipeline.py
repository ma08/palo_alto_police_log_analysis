#!/usr/bin/env python3
"""
Run the CSV-based extraction and analysis pipeline for police reports.
"""

import os
import sys
import time
from pathlib import Path

# Add src directory to Python path
sys.path.append(str(Path(__file__).parent / "src"))

# Import our modules
from src.markdown_to_csv import process_single_file, process_all_files
from src.analyze_csv_data import load_csv_files, clean_data, analyze_data, generate_visualizations, generate_comprehensive_report


def main():
    """
    Run the complete CSV-based extraction and analysis pipeline.
    """
    print("=" * 80)
    print("Starting Palo Alto Police Report Analysis (CSV Pipeline)")
    print("=" * 80)
    
    # Step 1: Process a single file as a test
    print("\nStep 1: Converting single markdown file to CSV as a test...")
    os.makedirs("data/csv_files", exist_ok=True)
    
    # Use a small file for testing
    test_file = "markitdown_output/april-01-2025-police-report-log.md"
    if os.path.exists(test_file):
        start_time = time.time()
        csv_file = process_single_file(test_file)
        test_time = time.time() - start_time
        
        if csv_file:
            print(f"Test conversion completed in {test_time:.2f} seconds")
            print(f"Test CSV file: {csv_file}")
        else:
            print("Test conversion failed. Please check your AWS credentials.")
            return
    else:
        print(f"Test file {test_file} not found. Make sure you have run the markdown conversion.")
        return
    
    # Continue automatically in non-interactive environments
    print("\nProceeding with processing all files...")
    
    # Step 2: Process all markdown files
    print("\nStep 2: Converting all markdown files to CSV...")
    start_time = time.time()
    csv_files = process_all_files("markitdown_output", "data/csv_files")
    conversion_time = time.time() - start_time
    print(f"Converted {len(csv_files)} files in {conversion_time:.2f} seconds")
    
    # Step 3: Combine and clean the CSV files
    print("\nStep 3: Combining and cleaning CSV files...")
    start_time = time.time()
    
    # Load and combine CSV files
    combined_df = load_csv_files("data/csv_files", "data/processed/combined_incidents.csv")
    
    if combined_df is None or combined_df.empty:
        print("No data was loaded from CSV files.")
        return
    
    # Clean the data
    cleaned_df = clean_data(combined_df)
    
    if cleaned_df is None or cleaned_df.empty:
        print("Failed to clean the combined data.")
        return
    
    cleaning_time = time.time() - start_time
    print(f"Combined and cleaned data in {cleaning_time:.2f} seconds")
    
    # Step 4: Analyze the data
    print("\nStep 4: Analyzing the data...")
    start_time = time.time()
    
    # Perform analysis
    stats = analyze_data(cleaned_df)
    
    if stats is None:
        print("Analysis failed to generate results.")
        return
    
    analysis_time = time.time() - start_time
    print(f"Analysis completed in {analysis_time:.2f} seconds")
    
    # Step 5: Generate visualizations and report
    print("\nStep 5: Generating visualizations and report...")
    start_time = time.time()
    
    # Create visualizations
    visualization_paths = generate_visualizations(cleaned_df, stats)
    
    # Generate comprehensive report
    report_path = generate_comprehensive_report(cleaned_df, stats)
    
    if not report_path:
        print("Failed to generate report.")
        return
    
    reporting_time = time.time() - start_time
    print(f"Generated report and visualizations in {reporting_time:.2f} seconds")
    
    # Summary
    print("\n" + "=" * 80)
    print("Pipeline Completed Successfully!")
    print("=" * 80)
    print(f"Processed {len(csv_files)} files")
    print(f"Extracted {len(cleaned_df)} incidents")
    print(f"Final report: {report_path}")
    print(f"Visualizations: {', '.join(visualization_paths)}")
    print("\nTotal processing time: {:.2f} seconds".format(
        test_time + conversion_time + cleaning_time + analysis_time + reporting_time
    ))


if __name__ == "__main__":
    main()