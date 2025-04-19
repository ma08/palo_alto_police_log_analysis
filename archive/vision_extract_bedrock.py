#!/usr/bin/env python3
"""
vision_extract_bedrock.py - Extract data from police report PDFs using Claude 3.7 on AWS Bedrock.
"""

import os
import base64
import json
import time
import glob
import pandas as pd
from tqdm import tqdm
import fitz  # PyMuPDF
import dotenv
from io import BytesIO
from PIL import Image
from anthropic import AnthropicBedrock

# Load environment variables
dotenv.load_dotenv()

# Constants
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
IMAGE_DIR = os.path.join(DATA_DIR, "images")
OUTPUT_CSV = os.path.join(PROCESSED_DATA_DIR, "police_reports.csv")

def ensure_directories_exist():
    """Ensure the necessary directories exist."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    os.makedirs(IMAGE_DIR, exist_ok=True)

def convert_pdf_to_images(pdf_path, output_dir=None, resolution=300):
    """Convert PDF pages to images using PyMuPDF."""
    if output_dir is None:
        output_dir = IMAGE_DIR
    
    pdf_filename = os.path.basename(pdf_path)
    base_filename = os.path.splitext(pdf_filename)[0]
    
    try:
        # Open the PDF
        doc = fitz.open(pdf_path)
        images = []
        
        # Convert each page to an image
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            
            # Render page to an image with higher resolution
            pix = page.get_pixmap(matrix=fitz.Matrix(resolution/72, resolution/72))
            
            # Save the image
            image_path = os.path.join(output_dir, f"{base_filename}_page_{page_num+1}.png")
            pix.save(image_path)
            images.append(image_path)
        
        print(f"Converted {pdf_filename} to {len(images)} images")
        return images
    
    except Exception as e:
        print(f"Error converting {pdf_filename} to images: {e}")
        return []

def encode_image(image_path):
    """Encode image to base64."""
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except Exception as e:
        print(f"Error encoding image {image_path}: {e}")
        return None

def resize_image_if_needed(image_path, max_size_mb=5):
    """Resize image if it's larger than max_size_mb."""
    try:
        # Check file size
        file_size_mb = os.path.getsize(image_path) / (1024 * 1024)
        if file_size_mb <= max_size_mb:
            return image_path
        
        # If file is too large, resize it
        scale_factor = (max_size_mb / file_size_mb) ** 0.5
        img = Image.open(image_path)
        new_width = int(img.width * scale_factor)
        new_height = int(img.height * scale_factor)
        resized_img = img.resize((new_width, new_height))
        
        # Save resized image
        resized_path = image_path.replace('.png', '_resized.png')
        resized_img.save(resized_path)
        print(f"Resized {image_path} from {file_size_mb:.2f}MB to {os.path.getsize(resized_path)/(1024*1024):.2f}MB")
        return resized_path
    
    except Exception as e:
        print(f"Error resizing image {image_path}: {e}")
        return image_path

def extract_with_bedrock_claude(image_path, model_id=None):
    """Extract data from image using Claude on AWS Bedrock with AnthropicBedrock client."""
    # Use model from environment if available, otherwise use default
    if model_id is None:
        model_id = os.environ.get('CLAUDE_MODEL_ID', "anthropic.claude-3-7-sonnet-20250219-v1:0")
        
    try:
        # Resize image if needed
        image_path = resize_image_if_needed(image_path)
        
        # Encode image
        base64_image = encode_image(image_path)
        if not base64_image:
            return None
        
        # Create AnthropicBedrock client
        client = AnthropicBedrock(
            aws_access_key=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            aws_region=os.environ.get('AWS_REGION', 'us-east-1'),
        )
        
        # Construct prompt for Claude
        prompt = """
        You are a data extraction expert. Extract ALL police incident records directly from the table in this image. 
        
        Look at the image carefully. The table has columns like CASE #, DATE, TIME, OFFENSE, LOCATION.
        
        IMPORTANT INSTRUCTIONS:
        - Extract EXACTLY what you see in the table without making anything up
        - Include every single row of actual incident data
        - Do NOT include headers or empty rows
        - COPY the text values EXACTLY as shown (including case numbers, dates, etc.)
        
        Format your response as a JSON array of objects with these fields:
        - case_number: The exact text from the CASE # column
        - date: The exact text from the DATE column
        - time: The exact text from the TIME column  
        - offense: The exact text from the OFFENSE column
        - location: The exact text from the LOCATION column
        
        Example of correct extraction if you see this in the table:
        25-01443 | 4/13/2025 | 1525 | Mental Health Evaluation | COWPER ST
        
        The JSON for that would be:
        {
          "case_number": "25-01443",
          "date": "4/13/2025",
          "time": "1525",
          "offense": "Mental Health Evaluation",
          "location": "COWPER ST"
        }
        
        Return ONLY a valid JSON array with all the records. No explanations or text outside the JSON.
        """
        
        # Send the request to Bedrock using AnthropicBedrock client
        response = client.messages.create(
            model=model_id,
            max_tokens=4096,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": base64_image
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ]
        )
        
        # Extract the response text
        extracted_text = response.content[0].text
        
        # Try to parse the JSON from the response
        try:
            # Find JSON array in the response if it's not clean
            if not extracted_text.strip().startswith('['):
                import re
                match = re.search(r'\[\s*{.*}\s*\]', extracted_text, re.DOTALL)
                if match:
                    extracted_text = match.group(0)
            
            # Parse the JSON
            data = json.loads(extracted_text)
            
            # Filter out any header rows or incomplete data
            filtered_data = []
            for record in data:
                # Skip records with case numbers like "ARREST CHARGES", "ARRES", etc.
                if record.get('case_number') and len(record.get('case_number', '')) > 3:
                    if not record.get('case_number').upper().startswith(('ARREST', 'CASE')):
                        filtered_data.append(record)
            
            return filtered_data
        except json.JSONDecodeError:
            print(f"Error parsing JSON from Claude's response for {image_path}")
            print(f"Response: {extracted_text[:500]}...")
            return None
    
    except Exception as e:
        print(f"Error extracting data from {image_path}: {e}")
        return None

def extract_street_name(location):
    """Extract the street name from a location string."""
    if not location or not isinstance(location, str):
        return None
        
    import re
    
    # Common street suffixes
    suffixes = ['ST', 'AVE', 'BLVD', 'RD', 'DR', 'CT', 'LN', 'WAY', 'PL', 'CIR']
    
    # Try to find a street suffix
    for suffix in suffixes:
        # Look for the suffix followed by a space or end of string
        pattern = rf'\b{suffix}\b'
        match = re.search(pattern, location.upper())
        if match:
            # Find the start of the street name (likely after a number)
            address_parts = location.split()
            for i, part in enumerate(address_parts):
                if part.upper() == suffix:
                    # Look backwards for the street name
                    street_parts = []
                    j = i - 1
                    # Skip the street number
                    while j >= 0 and not re.match(r'^\d+$', address_parts[j]):
                        street_parts.insert(0, address_parts[j])
                        j -= 1
                    if street_parts:
                        return ' '.join(street_parts + [suffix])
    
    # If no street suffix found, try to parse based on common patterns
    # For intersections like "ALMA ST & HAMILTON AVE"
    intersection_match = re.search(r'([A-Za-z\s]+)(?:\s+&\s+|\s+and\s+)([A-Za-z\s]+)', location)
    if intersection_match:
        return intersection_match.group(1).strip()
        
    # For block addresses like "600 block of FOREST AVE"
    block_match = re.search(r'block\s+of\s+([A-Za-z\s]+)', location)
    if block_match:
        return block_match.group(1).strip()
    
    # Default to returning the whole location if we can't find a specific street
    return location

def normalize_categories(offense):
    """Normalize offense categories."""
    if not offense or not isinstance(offense, str):
        return "Unknown"
    
    offense = offense.lower()
    
    if any(term in offense for term in ['theft', 'burglary', 'robbery', 'shoplifting', 'stolen']):
        return "Theft"
    elif any(term in offense for term in ['assault', 'battery', 'fight', 'violence']):
        return "Assault"
    elif any(term in offense for term in ['drug', 'narcotic', 'possession']):
        return "Drugs"
    elif any(term in offense for term in ['dui', 'driving under', 'alcohol', 'intoxicated']):
        return "DUI/Alcohol"
    elif any(term in offense for term in ['vandalism', 'graffiti', 'property damage']):
        return "Vandalism"
    elif any(term in offense for term in ['traffic', 'collision', 'accident', 'vehicle']):
        return "Traffic"
    elif any(term in offense for term in ['mental', 'welfare', 'health']):
        return "Mental Health"
    elif any(term in offense for term in ['trespass', 'suspicious', 'disturb']):
        return "Disturbance"
    else:
        return "Other"

def normalize_records(records):
    """Normalize the extracted records."""
    normalized_records = []
    
    for record in records:
        # Extract street name from location
        location = record.get('location', '')
        street_name = extract_street_name(location)
        
        # Normalize offense category
        offense = record.get('offense', '')
        offense_category = normalize_categories(offense)
        
        # Create normalized record
        normalized_record = {
            'CASE #': record.get('case_number', ''),
            'DATE': record.get('date', ''),
            'TIME': record.get('time', ''),
            'OFFENSE': offense,
            'LOCATION': location,
            'STREET_NAME': street_name,
            'OFFENSE_CATEGORY': offense_category,
            'SOURCE_FILE': record.get('source_file', '')
        }
        
        normalized_records.append(normalized_record)
    
    return normalized_records

def analyze_with_llm(records, model_id=None):
    """Use Claude to analyze patterns in the data."""
    # Use model from environment if available, otherwise use default
    if model_id is None:
        model_id = os.environ.get('CLAUDE_MODEL_ID', "anthropic.claude-3-7-sonnet-20250219-v1:0")
        
    try:
        # Create AnthropicBedrock client
        client = AnthropicBedrock(
            aws_access_key=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            aws_region=os.environ.get('AWS_REGION', 'us-east-1'),
        )
        
        # Convert records to JSON for prompt
        records_json = json.dumps(records[:100])  # Limit to 100 records for token constraints
        
        # Construct prompt for analysis
        prompt = f"""
        You are a crime data analyst helping a family find the safest neighborhood in Palo Alto. 
        Analyze the following police report data and identify patterns, trends, and insights.
        
        Data:
        {records_json}
        
        Please provide:
        1. A summary of the safest areas (streets/neighborhoods) based on crime frequency and severity
        2. Areas with higher than average incidents
        3. Temporal patterns (time of day, day of week, etc.) if apparent
        4. Types of crimes that are most common in different areas
        5. Specific recommendations for a family of 3 looking to move to Palo Alto
        
        Format your response as a JSON object with these sections, providing detailed analysis in each:
        {{"safest_areas": [...], "concerning_areas": [...], "temporal_patterns": [...], "crime_patterns": [...], "recommendations": [...]}}
        """
        
        # Send the request to Bedrock using AnthropicBedrock client
        response = client.messages.create(
            model=model_id,
            max_tokens=4096,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Extract the response text
        analysis_text = response.content[0].text
        
        # Try to parse the JSON from the response
        try:
            analysis_data = json.loads(analysis_text)
            return analysis_data
        except json.JSONDecodeError:
            print("Error parsing JSON from Claude's analysis response")
            return {"error": "Failed to parse analysis response"}
    
    except Exception as e:
        print(f"Error analyzing data with LLM: {e}")
        return {"error": str(e)}

def save_results(records, analysis=None):
    """Save the extracted records and analysis to files."""
    if not records:
        print("No records to save")
        return
    
    # Convert records to DataFrame
    df = pd.DataFrame(records)
    
    # Write to CSV
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(df)} records to {OUTPUT_CSV}")
    
    # Save analysis if provided
    if analysis:
        analysis_path = os.path.join(PROCESSED_DATA_DIR, "llm_analysis.json")
        with open(analysis_path, 'w') as f:
            json.dump(analysis, f, indent=2)
        print(f"Saved LLM analysis to {analysis_path}")
        
        # Create a markdown version of the analysis
        markdown_path = os.path.join(PROCESSED_DATA_DIR, "llm_analysis.md")
        with open(markdown_path, 'w') as f:
            f.write("# LLM Analysis of Palo Alto Police Reports\n\n")
            
            f.write("## Safest Areas\n")
            for area in analysis.get('safest_areas', []):
                f.write(f"- {area}\n")
            
            f.write("\n## Areas with More Safety Concerns\n")
            for area in analysis.get('concerning_areas', []):
                f.write(f"- {area}\n")
            
            f.write("\n## Temporal Patterns\n")
            for pattern in analysis.get('temporal_patterns', []):
                f.write(f"- {pattern}\n")
            
            f.write("\n## Crime Patterns\n")
            for pattern in analysis.get('crime_patterns', []):
                f.write(f"- {pattern}\n")
            
            f.write("\n## Recommendations for Families\n")
            for rec in analysis.get('recommendations', []):
                f.write(f"- {rec}\n")
        
        print(f"Saved markdown analysis to {markdown_path}")

def test_bedrock_models():
    """Test available Bedrock models."""
    import boto3
    import json

    try:
        bedrock = boto3.client(
            service_name='bedrock',
            region_name=os.environ.get('AWS_REGION', 'us-east-1'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
        )
        
        # Get available models
        response = bedrock.list_foundation_models(
            byProvider='anthropic'
        )
        
        print("Available Anthropic models on AWS Bedrock:")
        for model in response['modelSummaries']:
            print(f"- {model['modelId']}")
            
        return response['modelSummaries']
    
    except Exception as e:
        print(f"Error testing Bedrock models: {e}")
        return None

def process_single_pdf(pdf_file, page_limit=1):
    """Process a single PDF file with vision extraction."""
    # Setup directories
    ensure_directories_exist()
    
    # Path to the PDF
    pdf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "raw", pdf_file)
    
    print(f"Testing vision extraction on: {pdf_path}")
    
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found at {pdf_path}")
        return
    
    # Convert PDF to images
    image_paths = convert_pdf_to_images(pdf_path)
    
    if not image_paths:
        print("No images were generated from the PDF")
        return
    
    # Limit pages if specified
    if page_limit > 0:
        image_paths = image_paths[:page_limit]
    
    print(f"Processing {len(image_paths)} pages from the PDF")
    
    all_records = []
    
    # Process pages
    for i, image_path in enumerate(image_paths):
        print(f"Extracting from page {i+1}/{len(image_paths)}...")
        
        # Extract data from the image
        records = extract_with_bedrock_claude(image_path)
        
        if records:
            print(f"Successfully extracted {len(records)} records from page {i+1}")
            if len(records) > 0:
                print(json.dumps(records, indent=2))
                
            # Add source file information
            for record in records:
                record['source_file'] = os.path.basename(pdf_path)
                
            all_records.extend(records)
        else:
            print(f"No records extracted from page {i+1}")
    
    if all_records:
        print(f"Total records extracted: {len(all_records)}")
        
        # Print extracted data
        print("Extracted records:")
        for record in all_records:
            location = record.get('location', 'No location')
            offense = record.get('offense', 'Unknown')
            case = record.get('case_number', 'No case number')
            print(f"- Case {case}: {offense} at {location}")
        
        # Normalize records
        normalized = normalize_records(all_records)
        
        # Save to CSV for testing
        save_results(normalized)
        print(f"Saved {len(normalized)} records to CSV")
    else:
        print("No records were extracted from any page")

def main():
    """Main function."""
    print("Palo Alto Police Report Analysis - Vision Extraction")
    
    # Test Bedrock models
    print("\nTesting AWS Bedrock configuration...")
    test_bedrock_models()
    
    # Process a single PDF
    print("\nProcessing a sample PDF file...")
    process_single_pdf("april-18-2025-police-report-log.pdf", page_limit=1)
    
    print("\nVision extraction complete!")

if __name__ == "__main__":
    main()