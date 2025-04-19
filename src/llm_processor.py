#!/usr/bin/env python3
"""
Use an LLM to extract structured data from police report markdown files.
"""

import os
import json
import pandas as pd
import glob
from pathlib import Path
import boto3
from datetime import datetime
from time import sleep

# Check if AWS credentials are set
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, will use env vars directly


class BedrockProcessor:
    """Process markdown files using AWS Bedrock."""
    
    def __init__(self, model_id=None, region=None):
        """Initialize the processor with AWS credentials."""
        # Use model from environment if available, otherwise use default
        if model_id is None:
            model_id = os.environ.get('CLAUDE_MODEL_ID', "anthropic.claude-3-7-sonnet-20250219-v1:0")
        
        # Use region from environment if available, otherwise use default
        if region is None:
            region = os.environ.get('AWS_REGION', 'us-east-1')
        
        self.model_id = model_id
        self.client = boto3.client(
            service_name='bedrock-runtime',
            region_name=region,
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
        )
        
        print(f"Using model: {model_id} in region: {region}")
    
    def extract_incidents(self, markdown_text, file_name=None):
        """
        Extract incident data from markdown using AWS Bedrock.
        
        Args:
            markdown_text: The markdown text to process
            file_name: The name of the file being processed
            
        Returns:
            List of dictionaries containing incident data
        """
        prompt = f"""
You are an expert data extractor. Please extract structured police incident data from the text below.
The text is from a Palo Alto Police Department report log that has been converted from PDF to markdown.

Extract the following fields for each incident:
1. Case Number (format: 25-XXXXX)
2. Date (format: MM/DD/YYYY)
3. Time (format: HHMM, 24-hour)
4. Offense Type
5. Location
6. Any arrest information if available

Match each case number with its corresponding date, time, offense, and location.
Note that the data might be in a tabular format where all case numbers are listed first, then all dates, times, etc.
You need to align these correctly by finding the corresponding values at the same position in each section.

Return the data as a JSON array of objects with the following structure:
[
  {{
    "case_number": "25-12345",
    "date": "3/15/2025",
    "time": "1430",
    "offense_type": "Burglary - From motor vehicle (F)",
    "location": "123 University Ave",
    "arrest_info": "" // leave empty if not available
  }},
  ...
]

Include all incidents you can find in the data. Be precise in your extraction and make sure the case numbers match with their corresponding data.

Here is the content to process:
{markdown_text}
"""
        
        # If file name is provided, extract report date
        report_date = None
        if file_name:
            date_match = os.path.basename(file_name).split('.')[0]
            if date_match:
                report_date = date_match
        
        # Add report date to the prompt if available
        if report_date:
            prompt += f"\n\nNote: This report is from: {report_date}"
        
        # Prepare request
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4000,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.0
        })
        
        try:
            # Invoke the model with retries
            for attempt in range(3):
                try:
                    response = self.client.invoke_model(
                        body=body,
                        modelId=self.model_id,
                        accept="application/json",
                        contentType="application/json"
                    )
                    break
                except Exception as e:
                    if attempt < 2:  # Try again if not the last attempt
                        print(f"Attempt {attempt+1} failed: {e}, retrying in 5 seconds...")
                        sleep(5)
                    else:
                        raise
            
            # Parse the response
            response_body = json.loads(response.get('body').read().decode('utf-8'))
            content = response_body.get('content', [{}])[0].get('text', '')
            
            # Extract JSON from the response
            json_start = content.find('[')
            json_end = content.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = content[json_start:json_end]
                try:
                    incidents = json.loads(json_text)
                    return incidents
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON: {e}")
                    print(f"JSON text: {json_text[:100]}...")
                    return []
            else:
                print("Could not find JSON in response.")
                return []
                
        except Exception as e:
            print(f"Error invoking Bedrock model: {e}")
            return []


def process_markdown_files(markdown_dir="markitdown_output", output_csv="data/processed/llm_extracted.csv"):
    """
    Process all markdown files using LLM and compile results.
    
    Args:
        markdown_dir: Directory containing markdown files
        output_csv: Path to save the compiled CSV data
        
    Returns:
        DataFrame with all extracted incidents
    """
    # Check if AWS credentials are set
    if not os.environ.get('AWS_ACCESS_KEY_ID') or not os.environ.get('AWS_SECRET_ACCESS_KEY'):
        print("Error: AWS credentials not found in environment variables.")
        print("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
        return None
    
    # Initialize the processor
    processor = BedrockProcessor()
    
    # Find all markdown files
    markdown_files = glob.glob(f"{markdown_dir}/*.md")
    all_incidents = []
    
    for markdown_file in markdown_files:
        print(f"Processing {markdown_file}")
        
        try:
            # Read markdown file
            with open(markdown_file, 'r') as f:
                markdown_text = f.read()
            
            # Extract data using LLM
            incidents = processor.extract_incidents(markdown_text, file_name=markdown_file)
            
            # Add report date from file name
            date_match = os.path.basename(markdown_file).split('.')[0]
            for incident in incidents:
                incident['report_file'] = date_match
            
            all_incidents.extend(incidents)
            print(f"Extracted {len(incidents)} incidents from {markdown_file}")
            
        except Exception as e:
            print(f"Error processing {markdown_file}: {e}")
    
    if not all_incidents:
        print("No incidents extracted.")
        return None
    
    # Create DataFrame from all incidents
    df = pd.DataFrame(all_incidents)
    
    # Save to CSV
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False)
    print(f"Saved {len(df)} incidents to {output_csv}")
    
    return df


if __name__ == "__main__":
    # Process all markdown files and compile results
    df = process_markdown_files()
    
    if df is not None:
        print("\nExtraction Summary:")
        print(f"Total incidents extracted: {len(df)}")
        print(f"Unique case numbers: {df['case_number'].nunique()}")
        if 'location' in df.columns:
            print(f"Unique locations: {df['location'].nunique()}")
        print("\nSample data:")
        print(df.head())
    else:
        print("No data extracted.")