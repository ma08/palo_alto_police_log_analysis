# Palo Alto Police Report Analysis

This project analyzes police reports from Palo Alto to help identify safer neighborhoods for potential residents. It scrapes, processes, and analyzes police report data from the [Palo Alto Police Department's public logs](https://www.paloalto.gov/departments/police/public-information-portal/police-report-log).

## Latest Analysis Results (April 19, 2025)

Our most recent analysis has identified:

- **Areas with fewer safety concerns**: CLARK/WAY, PASTEUR/DR, TULIP, PRINCETON, Charleston, and Forest Ave
- **Areas with more incidents**: El Camino (58 incidents), Alma (24), and University Ave (18)
- **Areas with higher safety concerns**: Park, ROOSEVELT, CREEK/DR, Cambridge, and DANA
- **Common incident types**: Theft (38.2%), Other incidents (23.2%), Traffic incidents (15.5%)

> **UPDATE**: We've implemented a new LLM-powered pipeline using Microsoft's markitdown for better text extraction and Claude 3.7 Sonnet for structured data extraction. This improved our incident count from 37 to 401!

### Key Artifacts
- [CSV Safety Analysis](results/csv_safety_analysis.md) - Latest comprehensive report (recommended)
- [Top Incident Locations](results/csv_top_locations.png) - Streets with most incidents
- [Incident Types Distribution](results/csv_offense_categories.png) - Categories of incidents
- [Safety Score Comparison](results/csv_location_safety.png) - Areas by safety ranking
- [Incidents by Day of Week](results/csv_day_of_week.png) - Temporal patterns

## Project Overview

- Downloads police reports (PDFs) from the last 30 days
- Extracts structured data from PDF reports
- Analyzes crime frequency by location and type
- Generates visualizations and statistical summaries
- Produces a safety report with neighborhood recommendations

## End-to-End Process

1. **Data Collection**: 
   - The system downloads PDF reports from Palo Alto PD's public portal
   - Reports cover the period from March 19 to April 18, 2025

2. **Data Processing**:
   - **Markdown conversion**: PDFs are converted to markdown text using Microsoft's markitdown tool
   - **LLM extraction**: Structured data extracted using Claude 3.7 Sonnet via AWS Bedrock
   - Data is normalized and categorized by location and incident type
   - Streets and neighborhoods are identified from location data

3. **Data Analysis**:
   - Safety scores calculated based on incident frequency and severity
   - Weighted algorithm assigns higher scores to violent crimes
   - Areas ranked by relative safety concerns
   - Day-of-week patterns analyzed to identify temporal trends

4. **Visualization & Reporting**:
   - Charts generated for incident locations, types, and temporal patterns
   - Comprehensive safety report with neighborhood recommendations
   - Location safety comparison between safer and higher-concern areas
   - Incident type distribution by location

## Setup

```bash
# Clone the repository
git clone [repository-url]
cd palo_alto_police_report_analysis

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install 'markitdown[pdf]'  # For PDF to markdown conversion

# Configure AWS credentials for Claude
cp .env.example .env
# Edit .env with your AWS credentials
```

## Usage

You can run the entire CSV-based analysis pipeline with:

```bash
python run_csv_pipeline.py
```

This will:
1. Convert PDF reports to markdown text
2. Extract structured data into CSV using Claude
3. Analyze the data and generate visualizations
4. Create a comprehensive safety report

For running just the individual steps:

```bash
# Step 1: Convert PDFs to markdown
python -c "from src.markitdown_extractor import convert_pdf_to_markdown; convert_pdf_to_markdown('data/raw/your_file.pdf')"

# Step 2: Process markdown to CSV with Claude
python -c "from src.markdown_to_csv import process_single_file; process_single_file('markitdown_output/your_file.md')"

# Step 3: Analyze CSV data
python -c "from src.analyze_csv_data import load_csv_files, clean_data, analyze_data, generate_comprehensive_report; df = load_csv_files(); clean_df = clean_data(df); stats = analyze_data(clean_df); generate_comprehensive_report(clean_df, stats)"
```

## Results Directory

The analysis produces several outputs in the `results/` directory:

- `csv_safety_analysis.md`: Latest comprehensive safety report (April 19, 2025)
- `csv_top_locations.png`: Streets ranked by incident count
- `csv_offense_categories.png`: Distribution of incident types
- `csv_location_safety.png`: Safety concern score by location
- `csv_day_of_week.png`: Incident patterns throughout the week

Legacy outputs:
- `comprehensive_safety_analysis.md`: Previous vision-based analysis
- `summary_for_house_hunting.md`: Tailored recommendations for families
- `safety_report.md`: Previous technical summary with safety metrics

## Project Structure

- `src/`: Source code modules
  - `markdown_to_csv.py`: Processes markdown files with Claude to extract structured data
  - `markitdown_extractor.py`: Converts PDFs to markdown text
  - `analyze_csv_data.py`: Analyzes extracted data and generates visualizations
- `run_csv_pipeline.py`: Runs the entire CSV-based pipeline
- `data/`: Directory for data files
  - `raw/`: Raw PDF files
  - `csv_files/`: CSV data extracted from markdown
  - `processed/`: Combined and cleaned data
- `markitdown_output/`: Markdown versions of police report PDFs
- `results/`: Analysis outputs and visualizations
- `venv/`: Python virtual environment
- `requirements.txt`: Required Python packages
- `.env`: Configuration for AWS credentials

## Data Processing Notes

- **Geocoding Script (`scripts/process_all_csvs.py`)**: 
  - During the run on April 20, 2025, this script failed to process the following files due to parsing errors (incorrect number of fields detected on specific lines):
    - `data/csv_files/april-17-2025-police-report-log.csv` (Error on line 14)
    - `data/csv_files/march-31-2025-police-report-log.csv` (Error on line 6)
  - These files were skipped, but the other 20 CSVs in the directory were successfully processed and saved to `data/processed_csv_files`.
  - The geocoding results for successfully processed unique locations are cached in `data/geocoding_cache.json`.

## Data Sources

All data is sourced from the [Palo Alto Police Department's public information portal](https://www.paloalto.gov/departments/police/public-information-portal/police-report-log), which provides police report logs for the last 30 days.

## Improvements in This Version

- **New Data Extraction Pipeline**: Replaced vision-based extraction with a two-step process:
  1. Microsoft markitdown for better PDF text extraction
  2. Claude 3.7 Sonnet for structured data parsing
- **Better Data Normalization**: Improved street name extraction and offense categorization
- **More Comprehensive Analysis**: Increased from 37 to 401 incidents analyzed
- **Temporal Analysis**: Added day-of-week analysis to identify temporal patterns
- **Deeper Location-Based Insights**: Improved breakdown of incident types by location

## Limitations

- Analysis is limited to the last 30 days of reports
- Street name extraction may not be perfect in all cases
- Safety scoring is a simplified model based on incident type and frequency
- Some incidents may have been missed or duplicated in the extraction process
- This data should be used as one of many factors in housing decisions