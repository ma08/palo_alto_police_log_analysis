#!/usr/bin/env python3
"""
Convert markdown police reports to CSV files using LLM.
"""

import os
import sys
import json
import boto3
from dotenv import load_dotenv
from pathlib import Path
from time import sleep
from anthropic import AnthropicBedrock

# Load environment variables
load_dotenv()

class BedrockProcessor:
    """Process markdown files using AWS Bedrock with Anthropic client."""
    
    def __init__(self, model_id=None, region=None):
        """Initialize the processor with AWS credentials."""
        # Use model from environment if available, otherwise use default
        if model_id is None:
            model_id = os.environ.get('CLAUDE_MODEL_ID', "anthropic.claude-3-sonnet-20240229-v1:0")
        
        # Use region from environment if available, otherwise use default
        if region is None:
            region = os.environ.get('AWS_REGION', 'us-east-1')
        
        self.model_id = model_id
        self.client = AnthropicBedrock(
            aws_region=region,
            aws_access_key=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
        )
        
        print(f"Using model: {self.model_id} in region: {region} via AnthropicBedrock client")
    
    def markdown_to_csv(self, markdown_path, output_dir="data/csv_files"):
        """
        Convert a markdown police report to CSV using AWS Bedrock.
        
        Args:
            markdown_path: Path to the markdown file
            output_dir: Directory to save CSV output
            
        Returns:
            Path to the generated CSV file
        """
        # Read the markdown file
        with open(markdown_path, 'r') as f:
            markdown_text = f.read()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate output filename
        md_name = os.path.basename(markdown_path)
        csv_name = os.path.splitext(md_name)[0] + ".csv"
        output_path = os.path.join(output_dir, csv_name)
        
        prompt = f"""
You are an expert data extractor for police reports. Your task is to extract structured data from the Palo Alto Police Department report log text below.

The markdown text may have columns that are incorrectly aligned. The typical fields in each report are:
1. Case Number (format: 25-XXXXX)
2. Date (format: MM/DD/YYYY)
3. Time (format: HHMM, 24-hour)
4. Offense Type
5. Location
6. Arrestee information (if available)

Please reorganize this data into a well-structured CSV format. The data might be organized in sections where all case numbers are listed first, then all dates, then all times, etc. You need to match up each row's information correctly.

Here is the content to process:
```
{markdown_text}
```

Respond with ONLY the CSV data with these columns:
case_number,date,time,offense_type,location,arrest_info

Include a header row. If any field is unavailable for a particular incident, leave it empty. Your response should be ONLY the CSV data with no additional text, commentary, or explanation.
"""
        
        # Prepare request parameters for Anthropic client
        messages = [{"role": "user", "content": prompt}]
        max_tokens = 4000
        temperature = 0.0
        
        try:
            # Invoke the model with retries using the new client
            for attempt in range(3):
                try:
                    # Use client.messages.create
                    response = self.client.messages.create(
                        model=self.model_id,
                        max_tokens=max_tokens,
                        messages=messages,
                        temperature=temperature
                    )
                    break
                except Exception as e:
                    if attempt < 2:  # Try again if not the last attempt
                        print(f"Attempt {attempt+1} failed: {e}, retrying in 5 seconds...")
                        sleep(5)
                    else:
                        raise
            
            # Parse the response from Anthropic client
            # Response structure is different: response.content is a list of ContentBlock objects
            if response.content and len(response.content) > 0:
                 content = response.content[0].text
            else:
                 content = "" # Handle empty response case
            
            # Save the CSV data
            with open(output_path, 'w', newline='') as f:
                f.write(content)
            
            print(f"Successfully converted {markdown_path} to {output_path}")
            return output_path
                
        except Exception as e:
            print(f"Error processing {markdown_path}: {e}")
            return None


def process_all_files(markdown_dir="markitdown_output", output_dir="data/csv_files"):
    """
    Process all markdown files in a directory.
    
    Args:
        markdown_dir: Directory containing markdown files
        output_dir: Directory to save CSV files
        
    Returns:
        List of paths to generated CSV files
    """
    # Initialize the processor
    processor = BedrockProcessor()
    
    # Find all markdown files
    markdown_files = sorted(Path(markdown_dir).glob("*.md"))
    csv_files = []
    
    for markdown_file in markdown_files:
        print(f"Processing {markdown_file}")
        csv_file = processor.markdown_to_csv(str(markdown_file), output_dir)
        if csv_file:
            csv_files.append(csv_file)
    
    return csv_files


def process_single_file(markdown_file, output_dir="data/csv_files"):
    """
    Process a single markdown file.
    
    Args:
        markdown_file: Path to the markdown file
        output_dir: Directory to save CSV output
        
    Returns:
        Path to the generated CSV file
    """
    # Initialize the processor
    processor = BedrockProcessor()
    
    # Process the file
    return processor.markdown_to_csv(markdown_file, output_dir)


if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python markdown_to_csv.py <markdown_file or directory>")
        sys.exit(1)
    
    path = sys.argv[1]
    
    if os.path.isdir(path):
        # Process all files in directory
        csv_files = process_all_files(path)
        print(f"Processed {len(csv_files)} files.")
    else:
        # Process single file
        csv_file = process_single_file(path)
        if csv_file:
            print(f"Successfully processed {path} to {csv_file}")
        else:
            print(f"Failed to process {path}")