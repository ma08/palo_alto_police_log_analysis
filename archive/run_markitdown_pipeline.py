#!/usr/bin/env python3
"""
Run the complete markitdown extraction and analysis pipeline.
"""

import os
import sys
import time
from pathlib import Path

# Add src directory to Python path
sys.path.append(str(Path(__file__).parent / "src"))

# Import our modules
from src.markitdown_extractor import process_all_pdfs, refine_extraction_pattern
from src.analyze_markitdown_data import load_data, clean_data, analyze_data, generate_visualizations, generate_comprehensive_report


def main():
    """
    Run the complete markitdown extraction and analysis pipeline.
    """
    print("=" * 80)
    print("Starting Palo Alto Police Report Analysis Pipeline (Markitdown Version)")
    print("=" * 80)
    
    # Step 1: Extract data from PDFs using markitdown
    print("\nStep 1: Extracting data from PDFs...")
    start_time = time.time()
    
    # Process all PDFs and save to CSV
    df = process_all_pdfs(
        pdf_dir="data/raw",
        output_dir="markitdown_output",
        csv_output="data/processed/markitdown_extracted.csv"
    )
    
    if df is None or df.empty:
        print("No data extracted. If you need to refine the extraction pattern,")
        print("try running this first to analyze the markdown structure:")
        print("  from src.markitdown_extractor import refine_extraction_pattern")
        print("  refine_extraction_pattern('markitdown_output/some-file.md', verbose=True)")
        return
    
    extraction_time = time.time() - start_time
    print(f"Extraction completed in {extraction_time:.2f} seconds")
    
    # Step 2: Load and clean the data
    print("\nStep 2: Loading and cleaning data...")
    start_time = time.time()
    
    # Load from CSV
    extracted_df = load_data("data/processed/markitdown_extracted.csv")
    
    if extracted_df is None or extracted_df.empty:
        print("Failed to load extracted data.")
        return
    
    # Clean the data
    cleaned_df = clean_data(extracted_df)
    
    cleaning_time = time.time() - start_time
    print(f"Data cleaning completed in {cleaning_time:.2f} seconds")
    
    # Step 3: Analyze the data
    print("\nStep 3: Analyzing data...")
    start_time = time.time()
    
    # Perform analysis
    stats = analyze_data(cleaned_df)
    
    if stats is None:
        print("Analysis failed to generate results.")
        return
    
    analysis_time = time.time() - start_time
    print(f"Analysis completed in {analysis_time:.2f} seconds")
    
    # Step 4: Generate visualizations and report
    print("\nStep 4: Generating visualizations and report...")
    start_time = time.time()
    
    # Create visualizations
    visualization_paths = generate_visualizations(cleaned_df, stats)
    
    # Generate comprehensive report
    report_path = generate_comprehensive_report(cleaned_df, stats)
    
    if not report_path:
        print("Failed to generate report.")
        return
    
    reporting_time = time.time() - start_time
    print(f"Report generation completed in {reporting_time:.2f} seconds")
    
    # Summary
    print("\n" + "=" * 80)
    print("Pipeline Completed Successfully!")
    print("=" * 80)
    print(f"Extracted {len(extracted_df)} incidents using markitdown")
    print(f"Final report: {report_path}")
    print(f"Visualizations: {', '.join(visualization_paths)}")
    print("\nTotal processing time: {:.2f} seconds".format(
        extraction_time + cleaning_time + analysis_time + reporting_time
    ))


if __name__ == "__main__":
    main()