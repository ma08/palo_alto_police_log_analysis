#!/usr/bin/env python3
"""
Process a single markdown file using the LLM extraction pipeline.
"""

import os
import sys
import json
import boto3
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

def process_markdown_file(file_path):
    """
    Process a single markdown file using AWS Bedrock.
    
    Args:
        file_path: Path to the markdown file
        
    Returns:
        Extracted incidents
    """
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None
    
    # Read the file
    with open(file_path, 'r') as f:
        markdown_text = f.read()
    
    # Initialize Bedrock client
    client = boto3.client(
        service_name='bedrock-runtime',
        region_name=os.environ.get('AWS_REGION', 'us-east-1'),
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )
    
    # Create prompt
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
    
    model_id = os.environ.get('CLAUDE_MODEL_ID', "anthropic.claude-3-7-sonnet-20250219-v1:0")
    
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
        # Invoke the model
        print(f"Invoking model: {model_id}")
        response = client.invoke_model(
            body=body,
            modelId=model_id,
            accept="application/json",
            contentType="application/json"
        )
        
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
                return None
        else:
            print("Could not find JSON in response.")
            print(f"Response content: {content[:200]}...")
            return None
            
    except Exception as e:
        print(f"Error invoking Bedrock model: {e}")
        return None

if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) != 2:
        print("Usage: python process_single_file.py <markdown_file>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    print(f"Processing {file_path}...")
    
    incidents = process_markdown_file(file_path)
    
    if incidents:
        print(f"Extracted {len(incidents)} incidents:")
        for i, incident in enumerate(incidents[:5]):  # Show first 5 incidents
            print(f"\nIncident {i+1}:")
            for key, value in incident.items():
                print(f"  {key}: {value}")
        
        # Save to JSON file
        output_file = f"{os.path.splitext(file_path)[0]}_extracted.json"
        with open(output_file, 'w') as f:
            json.dump(incidents, f, indent=2)
        print(f"\nFull results saved to {output_file}")
    else:
        print("No incidents extracted.")