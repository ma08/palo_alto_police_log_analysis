# Palo Alto Police Report Analysis Project - Summary

## Overview
This project aims to help a family moving to Palo Alto make informed decisions about neighborhood safety. The system analyzes police reports from the past 30 days to identify patterns in crime frequency, types, and locations.

## Features
- **Data Collection**: Downloads PDFs from the Palo Alto Police Department's public portal
- **Data Extraction**: Parses complex PDF files to extract structured incident data
- **Safety Analysis**: Calculates safety scores for different streets and neighborhoods
- **Visualization**: Creates charts and maps showing crime patterns
- **Reporting**: Generates a comprehensive safety report with recommendations

## Implementation Details
1. **Virtual Environment**: Python venv with all dependencies installed
2. **Directory Structure**:
   - `data/raw`: Stores downloaded PDF files
   - `data/processed`: Stores extracted CSV data
   - `results`: Stores analysis outputs and visualizations

3. **Core Scripts**:
   - `download_reports.py`: Downloads police report PDFs
   - `extract_data.py`: Extracts structured data from PDFs
   - `analyze_data.py`: Analyzes crime patterns and generates insights
   - `run_analysis.py`: Runs the entire pipeline

4. **Analysis Metrics**:
   - Location-based incident frequency
   - Crime type categorization
   - Time-of-day patterns
   - Safety scoring algorithm that considers both frequency and severity

## How to Use
1. Activate the virtual environment (`source venv/bin/activate`)
2. Run the complete analysis: `python run_analysis.py`
3. Review the generated safety report in `results/safety_report.md`
4. Examine visualizations for deeper insights

## Next Steps
- Expand the dataset by collecting historical data beyond 30 days
- Add geospatial mapping capabilities to visualize crime hotspots
- Develop a web interface for interactive exploration of the data
- Include demographic and property value data for more comprehensive neighborhood analysis