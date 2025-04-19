#!/usr/bin/env python3
"""
Extract text from PDFs using markitdown and process it into structured data.
"""

import os
import re
import json
import glob
import pandas as pd
from pathlib import Path
import subprocess
from datetime import datetime


def convert_pdf_to_markdown(pdf_path, output_dir="markitdown_output"):
    """
    Convert a PDF to Markdown using markitdown CLI tool.
    
    Args:
        pdf_path: Path to the PDF file
        output_dir: Directory to save the markdown output
        
    Returns:
        Path to the generated markdown file
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename
    pdf_name = os.path.basename(pdf_path)
    md_name = os.path.splitext(pdf_name)[0] + ".md"
    output_path = os.path.join(output_dir, md_name)
    
    # Run markitdown command - note the syntax from the error message
    cmd = ["markitdown", pdf_path, "-o", output_path]
    print(f"Running: {' '.join(cmd)}")
    
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"Successfully converted {pdf_path} to {output_path}")
        return output_path
    except subprocess.CalledProcessError as e:
        print(f"Error converting {pdf_path}: {e}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        return None


def extract_incident_data(markdown_path):
    """
    Extract structured incident data from a markdown file.
    
    Args:
        markdown_path: Path to the markdown file
        
    Returns:
        List of dictionaries containing incident data
    """
    with open(markdown_path, 'r') as f:
        content = f.read()
    
    # Extract date from filename
    date_match = re.search(r'(march|april)-(\d{2})-2025', os.path.basename(markdown_path))
    report_date = None
    if date_match:
        month = date_match.group(1)
        day = date_match.group(2)
        month_num = 3 if month.lower() == 'march' else 4
        report_date = f"2025-{month_num:02d}-{day}"
    
    # Based on the observed structure, cases are in format "25-XXXXX" followed by date and offense
    # The pattern extracts case number, date, and offense type
    incident_pattern = r'(25-\d{5})\s+(\d{1,2}/\d{1,2}/\d{4})\s+(\d{4})\s+(.*?)(?=\s+\S+\s+\S+\s+25-\d{5}|\s*$)'
    
    incidents = []
    
    # Find all matches in the content
    for match in re.finditer(incident_pattern, content, re.DOTALL):
        if len(match.groups()) >= 4:
            case_num, date, time, offense_type = match.groups()
            
            # Further clean and process the extracted data
            offense_type = offense_type.strip()
            
            # Try to extract location from the offense description
            location_match = re.search(r'-\s*([A-Za-z\s\d]+)(?:\s*\([FM]\))?$', offense_type)
            location = location_match.group(1).strip() if location_match else "Unknown"
            
            # Clean the offense type by removing the location if it was found
            if location_match:
                offense_type = offense_type[:location_match.start()].strip()
            
            incidents.append({
                'case_number': case_num,
                'date': date,
                'time': time,
                'offense_type': offense_type,
                'location': location,
                'report_date': report_date
            })
    
    # If the above pattern didn't work, try a simpler approach to extract just case numbers
    if not incidents:
        case_pattern = r'(25-\d{5})'
        date_pattern = r'(\d{1,2}/\d{1,2}/\d{4})'
        offense_pattern = r'([A-Za-z\s\-:]+\([FM]\))'
        
        case_matches = re.findall(case_pattern, content)
        date_matches = re.findall(date_pattern, content)
        offense_matches = re.findall(offense_pattern, content)
        
        # Make sure we have equal numbers of each to pair them together
        min_length = min(len(case_matches), len(date_matches), len(offense_matches))
        
        for i in range(min_length):
            incidents.append({
                'case_number': case_matches[i],
                'date': date_matches[i],
                'offense_type': offense_matches[i],
                'location': "Unknown",
                'report_date': report_date
            })
    
    return incidents


def process_all_pdfs(pdf_dir="data/raw", output_dir="markitdown_output", csv_output="data/processed/markitdown_extracted.csv"):
    """
    Process all PDFs in a directory using markitdown and compile results.
    
    Args:
        pdf_dir: Directory containing PDF files
        output_dir: Directory to save markdown files
        csv_output: Path to save the compiled CSV data
        
    Returns:
        DataFrame with all extracted incidents
    """
    # Find all PDFs
    pdf_files = glob.glob(f"{pdf_dir}/*.pdf")
    all_incidents = []
    
    for pdf_file in pdf_files:
        print(f"Processing {pdf_file}")
        # Convert PDF to markdown
        md_file = convert_pdf_to_markdown(pdf_file, output_dir)
        
        if md_file:
            # Extract data from markdown
            incidents = extract_incident_data(md_file)
            all_incidents.extend(incidents)
            print(f"Extracted {len(incidents)} incidents from {pdf_file}")
        else:
            print(f"Failed to convert {pdf_file}")
    
    if not all_incidents:
        print("No incidents extracted.")
        return None
    
    # Create DataFrame from all incidents
    df = pd.DataFrame(all_incidents)
    
    # Save to CSV
    os.makedirs(os.path.dirname(csv_output), exist_ok=True)
    df.to_csv(csv_output, index=False)
    print(f"Saved {len(df)} incidents to {csv_output}")
    
    return df


def refine_extraction_pattern(markdown_path, verbose=False):
    """
    Analyze a markdown file to refine the extraction pattern.
    Helps in understanding the structure of the converted markdown.
    
    Args:
        markdown_path: Path to the markdown file
        verbose: Whether to print the content
        
    Returns:
        The content of the file
    """
    with open(markdown_path, 'r') as f:
        content = f.read()
    
    if verbose:
        print("\n" + "="*40 + " MARKDOWN CONTENT " + "="*40)
        print(content[:2000] + "..." if len(content) > 2000 else content)
        print("="*90 + "\n")
    
    # Print some statistics that might help understand the structure
    print(f"Content length: {len(content)} characters")
    print(f"Number of lines: {content.count('\n')}")
    
    # Look for potential patterns
    date_patterns = re.findall(r'\d{1,2}/\d{1,2}/\d{2,4}', content)
    case_patterns = re.findall(r'\d{5}-?\d*', content)
    
    print(f"Potential dates found: {len(date_patterns)}")
    print(f"Potential case numbers found: {len(case_patterns)}")
    
    return content


if __name__ == "__main__":
    # Process all PDFs and compile results
    df = process_all_pdfs()
    
    if df is not None:
        print("\nExtraction Summary:")
        print(f"Total incidents extracted: {len(df)}")
        print(f"Unique case numbers: {df['case_number'].nunique()}")
        print(f"Unique locations: {df['location'].nunique()}")
        print("\nSample data:")
        print(df.head())
    else:
        print("No data extracted.")