#!/usr/bin/env python3
"""
vision_extract.py - Extract data from police report PDFs using Claude vision capabilities.
"""

import os
import base64
import json
import time
import glob
import pandas as pd
from tqdm import tqdm
import fitz  # PyMuPDF
import boto3
import dotenv
from io import BytesIO
from PIL import Image

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
    """Extract data from image using Claude on AWS Bedrock."""
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
        
        # Create Bedrock client
        bedrock = boto3.client(
            service_name='bedrock-runtime',
            region_name=os.environ.get('AWS_REGION', 'us-west-2'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
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
        
        # Construct the request
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "messages": [
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
        }
        
        # Send the request to Bedrock
        response = bedrock.invoke_model(
            modelId=model_id,
            body=json.dumps(request_body)
        )
        
        # Parse the response
        response_body = json.loads(response['body'].read())
        extracted_text = response_body['content'][0]['text']
        
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

def process_pdf_files():
    """Process all PDF files in the raw data directory."""
    ensure_directories_exist()
    
    # Get all PDF files
    pdf_files = glob.glob(os.path.join(RAW_DATA_DIR, "*.pdf"))
    
    if not pdf_files:
        print(f"No PDF files found in {RAW_DATA_DIR}")
        return []
    
    all_records = []
    
    for pdf_file in tqdm(pdf_files, desc="Processing PDFs"):
        pdf_filename = os.path.basename(pdf_file)
        
        # Convert PDF to images
        image_paths = convert_pdf_to_images(pdf_file)
        
        if not image_paths:
            continue
        
        pdf_records = []
        
        # Process each image with vision model
        for image_path in tqdm(image_paths, desc=f"Extracting from {pdf_filename}", leave=False):
            # Extract data from image
            records = extract_with_bedrock_claude(image_path)
            
            if records:
                # Add source file information
                for record in records:
                    record['source_file'] = pdf_filename
                
                pdf_records.extend(records)
            
            # Sleep to avoid rate limits
            time.sleep(2)
        
        all_records.extend(pdf_records)
        
        # Save intermediate results
        if pdf_records:
            intermediate_df = pd.DataFrame(pdf_records)
            intermediate_path = os.path.join(PROCESSED_DATA_DIR, f"{os.path.splitext(pdf_filename)[0]}_extracted.csv")
            intermediate_df.to_csv(intermediate_path, index=False)
            print(f"Extracted {len(pdf_records)} records from {pdf_filename}")
    
    return all_records

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

def analyze_with_llm(records, model_id=None):
    """Use Claude to analyze patterns in the data."""
    # Use model from environment if available, otherwise use default
    if model_id is None:
        model_id = os.environ.get('CLAUDE_MODEL_ID', "anthropic.claude-3-7-sonnet-20250219-v1:0")
    try:
        # Create Bedrock client
        bedrock = boto3.client(
            service_name='bedrock-runtime',
            region_name=os.environ.get('AWS_REGION', 'us-west-2'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
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
        
        # Construct the request
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }
        
        # Send the request to Bedrock
        response = bedrock.invoke_model(
            modelId=model_id,
            body=json.dumps(request_body)
        )
        
        # Parse the response
        response_body = json.loads(response['body'].read())
        analysis_text = response_body['content'][0]['text']
        
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

def main():
    """Main function to extract data from PDFs using vision models."""
    try:
        print("Starting vision-based extraction of police reports...")
        
        # Process PDF files with vision model
        records = process_pdf_files()
        
        if not records:
            print("No records extracted. Check if PDFs were properly downloaded.")
            return
        
        # Normalize records
        normalized_records = normalize_records(records)
        
        # Analyze with LLM if AWS credentials are available
        analysis = None
        if os.environ.get('AWS_ACCESS_KEY_ID') and os.environ.get('AWS_SECRET_ACCESS_KEY'):
            print("Performing LLM analysis of extracted data...")
            analysis = analyze_with_llm(normalized_records)
        else:
            print("Skipping LLM analysis (AWS credentials not found)")
        
        # Save results
        save_results(normalized_records, analysis)
        
        print("Vision-based extraction complete!")
    
    except Exception as e:
        print(f"Error in vision extraction pipeline: {e}")

if __name__ == "__main__":
    main()