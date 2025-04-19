#!/usr/bin/env python3
"""
quick_analysis.py - Generate a quick safety analysis from the extracted data.
"""

import os
import sys
import json
import pandas as pd
import subprocess

def load_data():
    """Load the existing police report data."""
    # Try to load the existing CSV
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    processed_dir = os.path.join(data_dir, "processed")
    csv_path = os.path.join(processed_dir, "police_reports.csv")
    
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path)
            print(f"Loaded {len(df)} records from {csv_path}")
            return df
        except Exception as e:
            print(f"Error loading data: {e}")
            return None
    else:
        print(f"Data file not found: {csv_path}")
        return None

def generate_safety_analysis(df):
    """Generate a safety analysis report from the data."""
    # Setup directories
    base_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(base_dir, "results")
    os.makedirs(results_dir, exist_ok=True)
    
    # Process the data
    if df is None or df.empty:
        print("No data to analyze")
        return None
    
    # Group by street name and offense category
    street_counts = df['STREET_NAME'].value_counts()
    offense_counts = df['OFFENSE_CATEGORY'].value_counts()
    
    # Calculate percentages for offense types
    total_offenses = offense_counts.sum()
    offense_percentages = (offense_counts / total_offenses * 100).round(1)
    
    # Create report
    report_path = os.path.join(results_dir, "quick_safety_analysis.md")
    
    with open(report_path, 'w') as f:
        f.write("# Palo Alto Safety Analysis for House Hunting\n\n")
        
        # Overview section
        f.write("## Analysis Overview\n")
        f.write(f"This analysis is based on {len(df)} police incidents extracted from Palo Alto Police Department reports.\n\n")
        
        # Top locations section
        f.write("## Areas by Incident Frequency\n")
        f.write("### Locations with More Incidents\n")
        for location, count in street_counts.head(10).items():
            if pd.notna(location):  # Skip None/NaN
                f.write(f"- **{location}**: {count} incidents\n")
        
        f.write("\n### Locations with Fewer Incidents\n")
        for location, count in street_counts.tail(10).items():
            if pd.notna(location) and count < 2:  # Skip None/NaN and only show locations with few incidents
                f.write(f"- **{location}**: {count} incident\n")
        
        # Offense types section
        f.write("\n## Types of Incidents\n")
        for offense, count in offense_counts.items():
            percentage = offense_percentages[offense]
            f.write(f"- **{offense}**: {count} incidents ({percentage}%)\n")
        
        # Safety recommendations section
        f.write("\n## Safety Recommendations for Families\n")
        
        # Derive some recommendations
        safest_areas = [loc for loc, count in street_counts.items() if pd.notna(loc) and count < 2]
        concerning_areas = [loc for loc, count in street_counts.nlargest(5).items() if pd.notna(loc)]
        
        f.write("\n### Areas with Fewer Safety Concerns\n")
        for area in safest_areas[:5]:
            f.write(f"- **{area}**: Lower incident frequency\n")
        
        f.write("\n### Areas with More Safety Concerns\n")
        for area in concerning_areas[:5]:
            f.write(f"- **{area}**: Higher incident frequency\n")
        
        f.write("\n## Additional Recommendations\n")
        f.write("- Visit neighborhoods at different times of day to get a feel for activity levels\n")
        f.write("- Speak with current residents about their safety experiences\n")
        f.write("- Consider proximity to schools, parks, and community services\n")
        f.write("- Look at street lighting and visibility in potential neighborhoods\n")
        f.write("- Research longer-term crime trends beyond this analysis period\n")
    
    print(f"Generated safety analysis report: {report_path}")
    return report_path

def main():
    """Main function to generate a quick safety analysis."""
    print("\nPalo Alto Police Report Quick Analysis\n")
    
    # Load existing data
    df = load_data()
    
    if df is not None:
        # Generate safety analysis
        report_path = generate_safety_analysis(df)
        
        if report_path:
            # Open results folder
            results_dir = os.path.dirname(report_path)
            try:
                if sys.platform == 'darwin':  # macOS
                    subprocess.run(['open', results_dir])
                elif sys.platform == 'win32':  # Windows
                    subprocess.run(['explorer', results_dir])
                else:  # Linux
                    subprocess.run(['xdg-open', results_dir])
                print(f"\nResults folder opened: {results_dir}")
            except Exception as e:
                print(f"\nResults saved to: {results_dir}")
            
            print("\nAnalysis completed successfully!")
            print(f"\nTo view the safety report, open:\n{report_path}")
    else:
        print("Analysis could not be completed due to missing data.")

if __name__ == "__main__":
    main()