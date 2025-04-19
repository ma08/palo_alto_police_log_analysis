#!/usr/bin/env python3
"""
run_analysis.py - Main script to run the entire police report analysis pipeline.
"""

import os
import sys
import time
import importlib.util
import subprocess

def check_dependencies():
    """Check if required dependencies are installed."""
    try:
        import requests
        import pdfplumber
        import pandas
        import matplotlib
        import seaborn
        import tqdm
        import bs4
        print("✓ All required dependencies are installed.")
        return True
    except ImportError as e:
        print(f"✗ Missing dependency: {e.name}")
        print("Please install dependencies by running: pip install -r requirements.txt")
        return False

def run_module(module_path):
    """Run a Python module."""
    module_name = os.path.basename(module_path).replace('.py', '')
    print(f"\n{'=' * 80}\nRunning {module_name}...\n{'=' * 80}\n")
    
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
    
    print(f"\n{'=' * 80}\n{module_name} completed.\n{'=' * 80}\n")
    time.sleep(1)  # Small pause between steps for readability

def main():
    """Main function to run the entire pipeline."""
    print("\nPalo Alto Police Report Analysis Pipeline\n")
    
    # Check dependencies
    if not check_dependencies():
        return
    
    # Get the script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Step 1: Download reports
    download_script = os.path.join(script_dir, 'download_reports.py')
    run_module(download_script)
    
    # Step 2: Extract data
    extract_script = os.path.join(script_dir, 'extract_data.py')
    run_module(extract_script)
    
    # Step 3: Analyze data
    analyze_script = os.path.join(script_dir, 'analyze_data.py')
    run_module(analyze_script)
    
    # Open results folder
    results_dir = os.path.join(script_dir, 'results')
    try:
        if sys.platform == 'darwin':  # macOS
            subprocess.run(['open', results_dir])
        elif sys.platform == 'win32':  # Windows
            subprocess.run(['explorer', results_dir])
        else:  # Linux
            subprocess.run(['xdg-open', results_dir])
        print(f"\nResults folder opened: {results_dir}")
    except Exception as e:
        print(f"\nResults saved to: {results_dir}")
        print(f"(Could not open folder automatically: {str(e)})")
    
    print("\nAnalysis pipeline completed successfully!")
    print("\nTo view the safety report, open:")
    print(f"{os.path.join(results_dir, 'safety_report.md')}")

if __name__ == "__main__":
    main()