#!/usr/bin/env python3
"""
Run the LLM-based extraction pipeline for police reports.
"""

import os
import sys
import time
from pathlib import Path

# Add src directory to Python path
sys.path.append(str(Path(__file__).parent / "src"))

# Import our modules
from src.markitdown_extractor import convert_pdf_to_markdown
from src.llm_processor import process_markdown_files
from src.analyze_markitdown_data import load_data, clean_data, analyze_data, generate_visualizations, generate_comprehensive_report


def main():
    """
    Run the LLM-based extraction and analysis pipeline.
    """
    print("=" * 80)
    print("Starting Palo Alto Police Report Analysis Pipeline (LLM Version)")
    print("=" * 80)
    
    # Step 1: Generate markdown files from PDFs
    print("\nStep 1: Converting PDFs to Markdown...")
    markdown_dir = "markitdown_output"
    os.makedirs(markdown_dir, exist_ok=True)
    
    start_time = time.time()
    pdf_files = Path("data/raw").glob("*.pdf")
    
    # Count how many files we have
    pdf_files = list(pdf_files)
    print(f"Found {len(pdf_files)} PDF files to process.")
    
    # Convert each PDF to markdown
    for pdf_file in pdf_files:
        print(f"Converting {pdf_file}...")
        md_file = convert_pdf_to_markdown(str(pdf_file), markdown_dir)
        if md_file:
            print(f"  - Created {md_file}")
        else:
            print(f"  - Failed to convert {pdf_file}")
    
    conversion_time = time.time() - start_time
    print(f"PDF conversion completed in {conversion_time:.2f} seconds")
    
    # Step 2: Extract data from markdown using LLM
    print("\nStep 2: Extracting data from markdown using LLM...")
    if not os.path.exists(markdown_dir) or not any(Path(markdown_dir).glob("*.md")):
        print("No markdown files found. Please run Step 1 first.")
        return
    
    start_time = time.time()
    df = process_markdown_files(markdown_dir, "data/processed/llm_extracted.csv")
    
    if df is None or df.empty:
        print("No data was extracted by the LLM. Please check your AWS credentials and try again.")
        return
    
    extraction_time = time.time() - start_time
    print(f"LLM extraction completed in {extraction_time:.2f} seconds")
    
    # Step 3: Clean and analyze the data
    print("\nStep 3: Cleaning and analyzing data...")
    start_time = time.time()
    
    # Load from CSV
    extracted_df = load_data("data/processed/llm_extracted.csv")
    
    if extracted_df is None or extracted_df.empty:
        print("Failed to load extracted data.")
        return
    
    # Clean the data
    cleaned_df = clean_data(extracted_df)
    
    if cleaned_df is None or cleaned_df.empty:
        print("Failed to clean the data.")
        return
    
    # Perform analysis
    stats = analyze_data(cleaned_df)
    
    if stats is None:
        print("Analysis failed to generate results.")
        return
    
    analysis_time = time.time() - start_time
    print(f"Data analysis completed in {analysis_time:.2f} seconds")
    
    # Step 4: Generate visualizations and report
    print("\nStep 4: Generating visualizations and report...")
    start_time = time.time()
    
    # Create visualizations
    visualization_paths = generate_visualizations(cleaned_df, stats, output_dir="results")
    
    # Generate comprehensive report
    report_path = generate_comprehensive_report(cleaned_df, stats, results_dir="results")
    
    if not report_path:
        print("Failed to generate report.")
        return
    
    reporting_time = time.time() - start_time
    print(f"Report generation completed in {reporting_time:.2f} seconds")
    
    # Summary
    print("\n" + "=" * 80)
    print("Pipeline Completed Successfully!")
    print("=" * 80)
    print(f"Extracted {len(extracted_df)} incidents using LLM")
    print(f"Final report: {report_path}")
    print(f"Visualizations: {', '.join(visualization_paths)}")
    print("\nTotal processing time: {:.2f} seconds".format(
        conversion_time + extraction_time + analysis_time + reporting_time
    ))


if __name__ == "__main__":
    main()