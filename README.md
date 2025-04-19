# Palo Alto Police Report Analysis

This project analyzes police reports from Palo Alto to help identify safer neighborhoods for potential residents. It scrapes, processes, and analyzes police report data from the [Palo Alto Police Department's public logs](https://www.paloalto.gov/departments/police/public-information-portal/police-report-log).

## Latest Analysis Results (April 19, 2025)

Our most recent analysis has identified:

- **Areas with fewer safety concerns**: Residential streets like Cowper St, Tasso St, Forest Ave
- **Areas with more incidents**: University Ave shows the highest incident rate and safety concern score
- **Common incident types**: Mental health incidents and disturbances are most frequent, followed by theft

> **UPDATE**: We've enhanced our data extraction capabilities with Claude 3.7 Sonnet Vision to better capture all incidents from the police report PDFs

### Key Artifacts
- [Safety Summary for House Hunting](results/summary_for_house_hunting.md) - Tailored recommendations for families
- [Safety Report](results/safety_report.md) - Detailed analysis of locations and incident types
- [Incident Map by Location](results/top_locations.png) - Visual representation of incidents by street
- [Incident Types](results/offense_categories.png) - Breakdown of incident categories

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
   - **Vision-based extraction**: PDFs are converted to images and processed using Claude Vision on AWS Bedrock
   - Fallback to traditional PDF parsing using pdfplumber when needed
   - Data is normalized and categorized by location and incident type
   - Streets and neighborhoods are identified from location data

3. **LLM-Enhanced Analysis**:
   - Claude used to identify patterns in semi-structured data
   - Safety scores calculated based on incident frequency and severity
   - Areas ranked by relative safety concerns
   - Time patterns analyzed (when available)

4. **Visualization & Reporting**:
   - Charts generated for incident locations and types
   - Safety report with neighborhood recommendations
   - Location safety analysis with weighted scoring
   - LLM-generated insights for family-specific recommendations

## Setup

```bash
# Clone the repository
git clone [repository-url]
cd palo_alto_police_report_analysis

# Activate virtual environment
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure AWS credentials for Claude Vision
cp .env.example .env
# Edit .env with your AWS credentials
```

## Usage

You can run the entire analysis pipeline with a single command:

```bash
python run_analysis.py
```

This will:
1. Download police report PDFs for the last 30 days
2. Extract structured data from the PDFs
3. Analyze the data and generate visualizations
4. Create a safety report with recommendations

For vision-based extraction using Claude (requires AWS credentials):

```bash
python vision_extract.py
```

Alternatively, you can run each step individually:

```bash
# Step 1: Download reports
python download_reports.py

# Step 2: Extract data (traditional method)
python extract_data.py
# OR: Vision-based extraction (preferred)
python vision_extract.py

# Step 3: Analyze data
python analyze_data.py
```

## Results Directory

The analysis produces several outputs in the `results/` directory:

- `summary_for_house_hunting.md`: Concise recommendations for home buyers (Added April 19, 2025)
- `safety_report.md`: A technical summary with safety metrics by location
- `top_locations.csv`: List of locations with the most incidents
- `offense_categories.csv`: Breakdown of incident types
- `location_safety_scores.csv`: Safety scores for each location
- Various visualizations (PNG format):
  - `top_locations.png`: Streets ranked by incident count
  - `offense_categories.png`: Distribution of incident types
  - `location_safety.png`: Safety concern score by location
  - `time_of_day.png`: Incident patterns throughout the day

## Project Structure

- `download_reports.py`: Downloads police report PDFs
- `extract_data.py`: Traditional extraction of data from PDFs
- `vision_extract.py`: Vision-based extraction using Claude on AWS Bedrock
- `analyze_data.py`: Generates statistics and visualizations
- `run_analysis.py`: Runs the entire pipeline
- `data/`: Directory for downloaded PDFs and extracted data
  - `raw/`: Raw PDF files
  - `images/`: Rendered PDF pages as images (for vision model)
  - `processed/`: Processed CSV data
- `results/`: Analysis outputs and visualizations
- `venv/`: Python virtual environment
- `requirements.txt`: Required Python packages
- `.env.example`: Example environment variables file
- `.env`: Configuration for AWS credentials (not committed to Git)

## Data Sources

All data is sourced from the [Palo Alto Police Department's public information portal](https://www.paloalto.gov/departments/police/public-information-portal/police-report-log), which provides police report logs for the last 30 days.

## Limitations

- Analysis is limited to the last 30 days of reports
- PDF extraction may not capture 100% of incidents perfectly
- Safety scoring is a simplified model and should be used as one of many factors in decision-making
- Some incidents may not be reported or included in these logs