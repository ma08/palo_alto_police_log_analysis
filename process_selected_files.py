#!/usr/bin/env python3
"""
Process selected markdown files and analyze the resulting CSV data.
"""

import os
import sys
import time
import glob
from pathlib import Path
import pandas as pd

# Add src directory to Python path
sys.path.append(str(Path(__file__).parent / "src"))

# Import our modules
from src.markdown_to_csv import process_single_file
from src.analyze_csv_data import clean_data, analyze_data, generate_visualizations, generate_comprehensive_report


def main():
    """
    Process selected files and analyze the data.
    """
    print("=" * 80)
    print("Starting Palo Alto Police Report Analysis (Selected Files)")
    print("=" * 80)
    
    # Selected files to process
    selected_files = [
        "markitdown_output/march-24-2025-police-report-log.md",  # Had the most incidents
        "markitdown_output/april-07-2025-police-report-log.md",  # Had the 2nd most incidents
        "markitdown_output/march-21-2025-police-report-log.md",  # Another file
    ]
    
    # Check if files exist
    for f in selected_files[:]:
        if not os.path.exists(f):
            print(f"File not found: {f}, removing from list")
            selected_files.remove(f)
    
    if not selected_files:
        print("No valid files to process.")
        return
    
    # Step 1: Process selected files
    print(f"\nStep 1: Processing {len(selected_files)} selected files...")
    os.makedirs("data/csv_files", exist_ok=True)
    
    start_time = time.time()
    csv_files = []
    
    for file in selected_files:
        print(f"Processing {file}...")
        csv_file = process_single_file(file)
        if csv_file:
            csv_files.append(csv_file)
    
    processing_time = time.time() - start_time
    print(f"Processed {len(csv_files)} files in {processing_time:.2f} seconds")
    
    if not csv_files:
        print("No CSV files were generated.")
        return
    
    # Step 2: Combine and analyze the CSV files
    print("\nStep 2: Combining and analyzing CSV files...")
    
    # Load each CSV file
    dfs = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            df['source_file'] = os.path.basename(csv_file)
            dfs.append(df)
            print(f"Loaded {len(df)} incidents from {csv_file}")
        except Exception as e:
            print(f"Error loading {csv_file}: {e}")
    
    if not dfs:
        print("No data loaded from CSV files.")
        return
    
    # Combine the dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"Combined {len(combined_df)} incidents from {len(dfs)} files")
    
    # Save the combined data
    combined_csv = "data/processed/selected_incidents.csv"
    os.makedirs(os.path.dirname(combined_csv), exist_ok=True)
    combined_df.to_csv(combined_csv, index=False)
    print(f"Saved combined data to {combined_csv}")
    
    # Clean the data
    cleaned_df = clean_data(combined_df)
    
    if cleaned_df is None or cleaned_df.empty:
        print("Failed to clean the data.")
        return
    
    # Analyze the data
    stats = analyze_data(cleaned_df)
    
    if stats is None:
        print("Analysis failed to generate results.")
        return
    
    # Generate visualizations
    visualization_paths = generate_visualizations(cleaned_df, stats, output_dir="results/selected")
    
    # Generate comprehensive report
    report_path = generate_comprehensive_report(cleaned_df, stats, results_dir="results/selected")
    
    if not report_path:
        print("Failed to generate report.")
        return
    
    # Summary
    print("\n" + "=" * 80)
    print("Processing Completed Successfully!")
    print("=" * 80)
    print(f"Processed {len(csv_files)} files")
    print(f"Extracted {len(cleaned_df)} incidents")
    print(f"Final report: {report_path}")
    print(f"Visualizations: {', '.join(visualization_paths)}")
    
    # Print some key findings
    if 'safety_scores' in stats and not stats['safety_scores'].empty:
        safety_df = stats['safety_scores']
        print("\nKey Findings:")
        print("Areas with fewer safety concerns:")
        for street in safety_df.head(3).index:
            print(f"  - {street}")
        
        print("\nAreas with more safety concerns:")
        for street in safety_df.tail(3).iloc[::-1].index:
            print(f"  - {street}")


if __name__ == "__main__":
    main()