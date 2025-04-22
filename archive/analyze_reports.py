import csv
import os

def analyze_middlefield_incidents(csv_directory, output_file):
    """
    Analyzes police report CSV files for incidents on Middlefield Road.

    Args:
        csv_directory (str): The path to the directory containing the CSV files.
        output_file (str): The path to the file where the report will be saved.
    """
    middlefield_incidents = []
    # Define expected headers (lowercase for case-insensitive comparison)
    expected_location_header = 'location'
    expected_incident_header = 'offense_type'

    # Ensure the directory exists
    if not os.path.isdir(csv_directory):
        print(f"Error: Directory not found: {csv_directory}")
        return

    for filename in os.listdir(csv_directory):
        if filename.lower().endswith(".csv"):
            file_path = os.path.join(csv_directory, filename)
            try:
                with open(file_path, mode='r', encoding='utf-8', errors='ignore') as csvfile:
                    # Use DictReader for easier column access
                    reader = csv.reader(csvfile)
                    # Read the header
                    try:
                        header = next(reader)
                    except StopIteration:
                        print(f"Skipping empty file: {filename}")
                        continue # Skip empty files

                    # Normalize header for case-insensitive matching
                    header_lower = [h.lower().strip() for h in header]

                    # Basic header validation (check if expected columns exist)
                    try:
                        location_index = header_lower.index(expected_location_header)
                        incident_index = header_lower.index(expected_incident_header)
                    except ValueError:
                        print(f"Warning: Skipping file '{filename}' due to missing '{expected_location_header}' or '{expected_incident_header}' header.")
                        continue

                    # Process rows
                    for i, row in enumerate(reader):
                        # Handle potential short rows
                        if len(row) > max(location_index, incident_index):
                            location = row[location_index].strip()
                            incident = row[incident_index].strip()
                            # Check if 'Middlefield' is in the location (case-insensitive)
                            if "middlefield" in location.lower():
                                middlefield_incidents.append({
                                    "File": filename,
                                    "Location": location,
                                    "Incident": incident,
                                    "Line": i + 2 # +1 for header, +1 for 0-based index
                                })
                        else:
                            print(f"Warning: Skipping short row {i+2} in file '{filename}'.")


            except FileNotFoundError:
                print(f"Warning: File not found: {file_path}")
            except Exception as e:
                print(f"Error processing file {filename}: {e}")

    # Generate the report
    report_content = """Incidents Reported on Middlefield Road
=====================================

"""

    if not middlefield_incidents:
        report_content += "No incidents found on Middlefield Road in the provided files.\n"
    else:
        # Sort incidents by file name, then by line number for consistency
        middlefield_incidents.sort(key=lambda x: (x['File'], x['Line']))
        current_file = None
        for incident in middlefield_incidents:
            if incident['File'] != current_file:
                if current_file is not None:
                    report_content += "\n" # Add space between files
                report_content += f"--- {incident['File']} ---\n"
                current_file = incident['File']
            report_content += f"- Location: {incident['Location']}\n"
            report_content += f"  Incident: {incident['Incident']}\n"


    # Write the report to the output file
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        print(f"Report successfully generated: {output_file}")
    except Exception as e:
        print(f"Error writing report file {output_file}: {e}")

if __name__ == "__main__":
    CSV_DIR = "data/csv_files"  # Relative path to the CSV directory
    REPORT_FILE = "middlefield_crimes_report.txt" # Output report file name
    analyze_middlefield_incidents(CSV_DIR, REPORT_FILE) 