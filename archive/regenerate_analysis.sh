#!/bin/bash
# Script to regenerate the full analysis

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Run the CSV pipeline
echo "Running CSV pipeline..."
python run_csv_pipeline.py

echo "Analysis complete! Results available in results/ directory"
echo "Main report: results/csv_safety_analysis.md"