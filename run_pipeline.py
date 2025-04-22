#!/usr/bin/env python3
"""
run_pipeline.py - Runs the full data processing pipeline for a given date range by calling imported functions, with an option to start from a specific step.
"""

import argparse
# import subprocess # No longer needed
import sys
import os
from datetime import datetime
import traceback

# --- Import main functions from pipeline scripts ---

# PROJECT_ROOT should still point to the directory containing run_pipeline.py
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
# No longer need sys.path manipulation as we'll use package imports

try:
    # Step 1: Download
    from pipeline.steps.step_1_download import main as download_main
    # Step 2: PDF to Markdown (using the function directly)
    from pipeline.steps.step_2_extract_text import process_all_pdfs
    # Step 3: Markdown to Raw CSVs (assuming main function exists in the step 3 module)
    from pipeline.steps.step_3_extract_structured import process_all_files as csv_pipeline_main
    # Step 4: Process CSVs (Geocode & Categorize)
    from pipeline.steps.step_4_process_data import run_processing as process_csvs_main
    # Step 5: Prepare Website Data
    from pipeline.steps.step_5_prepare_output import prepare_data_for_website
except ImportError as e:
    print(f"Error importing pipeline functions: {e}")
    print("Please ensure all pipeline scripts exist and are importable (check paths and syntax).")
    sys.exit(1)

def main_orchestrator(start_date_str, end_date_str, start_step=1):
    """Runs the end-to-end pipeline by calling imported functions, starting from start_step."""
    print(f"\n=== STARTING DATA PROCESSING PIPELINE (Starting from Step {start_step}) ===")

    # Define expected input/output directories based on project structure
    # These might need adjustment if individual scripts have different expectations
    pdf_input_dir = os.path.join(PROJECT_ROOT, "data", "raw_pdfs")
    markdown_output_dir = os.path.join(PROJECT_ROOT, "markitdown_output")
    # raw_csv_dir = os.path.join(PROJECT_ROOT, "data", "csv_files") # Unused variable removed
    # processed_csv_dir = os.path.join(PROJECT_ROOT, "data", "processed_csv_files") # Unused variable removed
    # website_data_output = os.path.join(PROJECT_ROOT, "website", "public", "data", "incidents.json") # Unused variable removed
    # Markitdown extractor also saves a combined CSV, specify its path
    md_extractor_csv_output = os.path.join(
        PROJECT_ROOT, "data", "processed", "markitdown_extracted.csv"
    )

    # --- Step 1: Download PDFs ---
    current_step = 1
    if start_step <= current_step:
        print(f"\n--- Running Step {current_step}: Download PDFs ---")
        try:
            download_main(start_date_str, end_date_str)
            print(f"--- Step {current_step} Completed Successfully ---")
        except Exception as e:
            print(f"--- Step {current_step} FAILED: Download PDFs ---")
            print(f"Error: {e}")
            traceback.print_exc()
            sys.exit(1)
    else:
        print(f"\n--- Skipping Step {current_step}: Download PDFs ---")

    # --- Step 2: PDF to Markdown ---
    current_step = 2
    if start_step <= current_step:
        print(f"\n--- Running Step {current_step}: PDF to Markdown Extraction ---")
        try:
            print(f"Input PDF directory: {pdf_input_dir}")
            print(f"Output Markdown directory: {markdown_output_dir}")
            print(f"Extractor combined CSV output: {md_extractor_csv_output}")
            # Call the function directly, passing the correct input and output directories
            result_df = process_all_pdfs(
                pdf_dir=pdf_input_dir,
                output_dir=markdown_output_dir,
                csv_output=md_extractor_csv_output
            )
            if result_df is None:
                print("Warning: PDF to Markdown step produced no data.") # Don't necessarily exit
            print(f"--- Step {current_step} Completed Successfully ---")
        except Exception as e:
            print(f"--- Step {current_step} FAILED: PDF to Markdown Extraction ---")
            print(f"Error: {e}")
            traceback.print_exc()
            sys.exit(1)
    else:
        print(f"\n--- Skipping Step {current_step}: PDF to Markdown Extraction ---")

    # --- Step 3: Markdown to CSV (via run_csv_pipeline) ---
    current_step = 3
    if start_step <= current_step:
        print(f"\n--- Running Step {current_step}: Markdown to Raw CSV (via run_csv_pipeline) ---")
        # Note: run_csv_pipeline.main might do MORE than just Md -> CSV (like analysis)
        # We assume it reads from markdown_output_dir and writes to raw_csv_dir implicitly for now
        try:
            csv_pipeline_main()
            print(f"--- Step {current_step} Completed Successfully ---")
        except Exception as e:
            print(f"--- Step {current_step} FAILED: Markdown to Raw CSV (via run_csv_pipeline) ---")
            print(f"Error: {e}")
            traceback.print_exc()
            sys.exit(1)
    else:
        print(f"\n--- Skipping Step {current_step}: Markdown to Raw CSV (via run_csv_pipeline) ---")

    # --- Step 4: Process CSVs (Geocode & Categorize) ---
    current_step = 4
    if start_step <= current_step:
        print(f"\n--- Running Step {current_step}: Process CSVs (Geocode & Categorize) ---")
        # Assumes process_csvs_main reads from raw_csv_dir and writes to processed_csv_dir implicitly
        try:
            process_csvs_main() # Call the correctly imported function
            print(f"--- Step {current_step} Completed Successfully ---")
        except Exception as e:
            print(f"--- Step {current_step} FAILED: Process CSVs (Geocode & Categorize) ---")
            print(f"Error: {e}")
            traceback.print_exc()
            sys.exit(1)
    else:
        print(f"\n--- Skipping Step {current_step}: Process CSVs (Geocode & Categorize) ---")

    # --- Step 5: Prepare Website Data ---
    current_step = 5
    if start_step <= current_step:
        print(f"\n--- Running Step {current_step}: Prepare Website Data ---")
        # Assumes prepare_data_for_website reads from processed_csv_dir and writes the json implicitly
        try:
            prepare_data_for_website()
            print(f"--- Step {current_step} Completed Successfully ---")
        except Exception as e:
            print(f"--- Step {current_step} FAILED: Prepare Website Data ---")
            print(f"Error: {e}")
            traceback.print_exc()
            sys.exit(1)
    else:
        print(f"\n--- Skipping Step {current_step}: Prepare Website Data ---")

    print("\n=== DATA PROCESSING PIPELINE COMPLETED SUCCESSFULLY ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the full Palo Alto Police Report processing pipeline by calling internal functions, optionally starting from a specific step."
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format."
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format."
    )
    parser.add_argument(
        "--start-step",
        type=int,
        default=1,
        choices=range(1, 6), # Only allow steps 1 through 5
        help="Pipeline step number to start from (1-5, default: 1)."
    )

    args = parser.parse_args()

    # Basic date validation
    try:
        start = datetime.strptime(args.start_date, "%Y-%m-%d")
        end = datetime.strptime(args.end_date, "%Y-%m-%d")
        if start > end:
            print("Error: Start date cannot be after end date.")
            sys.exit(1)
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format.")
        sys.exit(1)

    # Call the main orchestrator function, passing the start_step
    main_orchestrator(args.start_date, args.end_date, args.start_step) 