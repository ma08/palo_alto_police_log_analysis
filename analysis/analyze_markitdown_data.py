#!/usr/bin/env python3
"""
Analyze data extracted from PDFs using markitdown.
"""

import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime


def load_data(csv_path="data/processed/markitdown_extracted.csv"):
    """
    Load the extracted data from CSV.
    
    Args:
        csv_path: Path to the CSV file
        
    Returns:
        DataFrame with the extracted data
    """
    try:
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} incidents from {csv_path}")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


def clean_data(df):
    """
    Clean and normalize the data.
    
    Args:
        df: DataFrame with extracted data
        
    Returns:
        Cleaned DataFrame
    """
    if df is None or len(df) == 0:
        print("No data to clean.")
        return None
    
    # Make a copy to avoid SettingWithCopyWarning
    cleaned_df = df.copy()
    
    # Normalize date formats
    if 'date' in cleaned_df.columns:
        try:
            cleaned_df['date'] = pd.to_datetime(cleaned_df['date'], errors='coerce')
            # Fill NaT values with report_date if available
            if 'report_date' in cleaned_df.columns:
                # Convert report_date to datetime too
                cleaned_df['report_date'] = pd.to_datetime(cleaned_df['report_date'], errors='coerce')
                # Use report_date where date is NaT
                date_mask = cleaned_df['date'].isna()
                cleaned_df.loc[date_mask, 'date'] = cleaned_df.loc[date_mask, 'report_date']
        except Exception as e:
            print(f"Error normalizing dates: {e}")
    
    # Extract street names
    if 'location' in cleaned_df.columns:
        cleaned_df['street'] = cleaned_df['location'].apply(extract_street_name)
    
    # Categorize offense types
    if 'offense_type' in cleaned_df.columns:
        cleaned_df['offense_category'] = cleaned_df['offense_type'].apply(categorize_offense)
    
    return cleaned_df


def extract_street_name(location):
    """
    Extract the street name from a location string.
    
    Args:
        location: Location string
        
    Returns:
        Street name
    """
    if not location or not isinstance(location, str):
        return None
    
    # Common Palo Alto street names and patterns
    streets = [
        'Alma', 'University', 'Hamilton', 'Waverley', 'Bryant', 'Emerson', 'Ramona',
        'High', 'Cowper', 'Webster', 'Middlefield', 'El Camino', 'Page Mill', 'Oregon',
        'Charleston', 'Arastradero', 'San Antonio', 'Embarcadero', 'California', 'Cambridge',
        'Addison', 'Channing', 'Homer', 'Lytton', 'Everett', 'Park', 'Forest'
    ]
    
    # Try to match known street names
    for street in streets:
        if re.search(r'\b' + re.escape(street) + r'\b', location, re.IGNORECASE):
            return street
    
    # Try to extract streets with suffixes
    street_pattern = r'\b([A-Za-z]+)\s+(St|Ave|Blvd|Rd|Way|Dr|Ln|Ct|Pl|Cir)\b'
    match = re.search(street_pattern, location)
    if match:
        return match.group(1)
    
    # If no patterns match, return the first part of the location
    parts = location.split()
    if parts:
        return parts[0]
    
    return None


def categorize_offense(offense_type):
    """
    Categorize offense types into broader categories.
    
    Args:
        offense_type: Offense type string
        
    Returns:
        Offense category
    """
    if not offense_type or not isinstance(offense_type, str):
        return "Unknown"
    
    offense_lower = offense_type.lower()
    
    # Define categories and their keywords
    categories = {
        'Theft': ['theft', 'burglary', 'robbery', 'shoplifting', 'stolen'],
        'Traffic': ['traffic', 'vehicle', 'driving', 'dui', 'parking'],
        'Assault': ['assault', 'battery', 'fight', 'violence'],
        'Property Damage': ['vandalism', 'damage', 'graffiti'],
        'Drugs/Alcohol': ['drug', 'narcotics', 'alcohol', 'intoxication'],
        'Mental Health': ['mental', 'welfare', 'crisis'],
        'Noise/Disturbance': ['noise', 'disturbance', 'loud', 'party'],
        'Fraud': ['fraud', 'scam', 'identity theft', 'forgery'],
    }
    
    # Check if offense matches any category
    for category, keywords in categories.items():
        if any(keyword in offense_lower for keyword in keywords):
            return category
    
    return "Other"


def analyze_data(df):
    """
    Analyze cleaned data and generate statistics.
    
    Args:
        df: Cleaned DataFrame
        
    Returns:
        Dictionary with analysis results
    """
    if df is None or len(df) == 0:
        print("No data to analyze.")
        return None
    
    # Calculate statistics
    stats = {}
    
    # Count incidents by location/street
    if 'street' in df.columns:
        street_counts = df['street'].value_counts()
        stats['top_locations'] = street_counts.head(10).to_dict()
    
    # Count incidents by offense category
    if 'offense_category' in df.columns:
        offense_counts = df['offense_category'].value_counts()
        stats['offense_counts'] = offense_counts.to_dict()
    
    # Calculate safety scores (lower is better)
    if 'street' in df.columns and 'offense_category' in df.columns:
        # Define severity weights for different offense types
        severity_weights = {
            'Assault': 10,
            'Theft': 7,
            'Property Damage': 5,
            'Drugs/Alcohol': 4,
            'Fraud': 3,
            'Traffic': 2,
            'Noise/Disturbance': 2,
            'Mental Health': 1,
            'Other': 1
        }
        
        # Calculate weighted counts for each street
        street_safety = {}
        for street in df['street'].dropna().unique():
            street_df = df[df['street'] == street]
            weighted_score = 0
            for category, weight in severity_weights.items():
                category_count = len(street_df[street_df['offense_category'] == category])
                weighted_score += category_count * weight
            
            # Normalize by total number of incidents
            incident_count = len(street_df)
            if incident_count > 0:
                street_safety[street] = {
                    'safety_score': weighted_score / incident_count,
                    'incident_count': incident_count,
                    'weighted_score': weighted_score
                }
        
        # Convert to DataFrame for easier manipulation
        safety_df = pd.DataFrame.from_dict(street_safety, orient='index')
        # Filter streets with at least 2 incidents for more reliable scores
        safety_df = safety_df[safety_df['incident_count'] >= 2]
        # Sort by safety score (ascending)
        safety_df = safety_df.sort_values('safety_score')
        
        stats['safety_scores'] = safety_df
    
    return stats


def generate_visualizations(df, stats, output_dir="results"):
    """
    Generate visualizations from the analysis.
    
    Args:
        df: Cleaned DataFrame
        stats: Dictionary with analysis results
        output_dir: Directory to save visualizations
        
    Returns:
        List of paths to generated visualization files
    """
    if df is None or len(df) == 0 or stats is None:
        print("No data to visualize.")
        return []
    
    os.makedirs(output_dir, exist_ok=True)
    visualization_paths = []
    
    # Set plot style
    sns.set_style("whitegrid")
    plt.rcParams.update({'font.size': 12})
    
    # 1. Top incident locations
    if 'top_locations' in stats:
        plt.figure(figsize=(12, 8))
        top_locations = pd.Series(stats['top_locations']).sort_values(ascending=True)
        bars = sns.barplot(x=top_locations.values, y=top_locations.index)
        
        # Add count labels
        for i, v in enumerate(top_locations.values):
            bars.text(v + 0.1, i, str(v), color='black', va='center')
        
        plt.title('Top 10 Streets by Number of Incidents')
        plt.xlabel('Number of Incidents')
        plt.tight_layout()
        
        top_locations_path = os.path.join(output_dir, 'markitdown_top_locations.png')
        plt.savefig(top_locations_path)
        plt.close()
        visualization_paths.append(top_locations_path)
    
    # 2. Offense categories distribution
    if 'offense_counts' in stats:
        plt.figure(figsize=(10, 8))
        offense_counts = pd.Series(stats['offense_counts']).sort_values(ascending=False)
        plt.pie(offense_counts, labels=offense_counts.index, autopct='%1.1f%%', 
                startangle=90, shadow=True)
        plt.axis('equal')
        plt.title('Distribution of Offense Categories')
        plt.tight_layout()
        
        categories_path = os.path.join(output_dir, 'markitdown_offense_categories.png')
        plt.savefig(categories_path)
        plt.close()
        visualization_paths.append(categories_path)
    
    # 3. Location safety scores (lower is better)
    if 'safety_scores' in stats:
        safety_df = stats['safety_scores']
        if not safety_df.empty and len(safety_df) >= 5:
            plt.figure(figsize=(12, 8))
            # Get top 10 safest and top 10 least safe streets
            safest = safety_df.head(10)
            least_safe = safety_df.tail(10).iloc[::-1]  # Reverse to show worst at top
            
            # Combine into one DataFrame for visualization
            plot_df = pd.concat([
                safest[['safety_score']].assign(category='Safer Areas'),
                least_safe[['safety_score']].assign(category='Areas of Concern')
            ])
            
            # Create grouped bar chart
            sns.barplot(x='safety_score', y=plot_df.index, hue='category', data=plot_df,
                      palette={'Safer Areas': 'green', 'Areas of Concern': 'red'})
            
            plt.title('Street Safety Comparison (Lower Score is Better)')
            plt.xlabel('Weighted Safety Score')
            plt.tight_layout()
            
            safety_path = os.path.join(output_dir, 'markitdown_location_safety.png')
            plt.savefig(safety_path)
            plt.close()
            visualization_paths.append(safety_path)
    
    return visualization_paths


def generate_comprehensive_report(df, stats, results_dir="results"):
    """
    Generate a comprehensive final report.
    
    Args:
        df: Cleaned DataFrame
        stats: Dictionary with analysis results
        results_dir: Directory to save the report
        
    Returns:
        Path to the generated report file
    """
    if df is None or len(df) == 0 or stats is None:
        print("No data to generate report.")
        return None
    
    # Extract key statistics
    top_locations = stats.get('top_locations', {})
    offense_counts = stats.get('offense_counts', {})
    safety_df = stats.get('safety_scores', None)
    
    # Create results directory if it doesn't exist
    os.makedirs(results_dir, exist_ok=True)
    
    # Generate report filename
    report_path = os.path.join(results_dir, 'markitdown_safety_analysis.md')
    
    with open(report_path, 'w') as f:
        # Write report header
        f.write("# Palo Alto Safety Analysis Report (Markitdown Extraction)\n\n")
        f.write(f"*Generated on: {datetime.now().strftime('%Y-%m-%d')}*\n\n")
        
        f.write("## Overview\n\n")
        f.write(f"This report analyzes {len(df)} police incidents extracted from Palo Alto Police Department logs ")
        date_range = ""
        if 'date' in df.columns:
            min_date = df['date'].min()
            max_date = df['date'].max()
            if pd.notna(min_date) and pd.notna(max_date):
                date_range = f"from {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}"
        f.write(f"{date_range}. ")
        f.write("The analysis is intended to help identify safer areas for housing in Palo Alto based on police incident reports.\n\n")
        
        # Incident Locations
        f.write("## Incident Locations\n\n")
        f.write("The following streets have the highest number of reported incidents:\n\n")
        
        for street, count in sorted(top_locations.items(), key=lambda x: x[1], reverse=True)[:10]:
            f.write(f"- **{street}**: {count} incidents\n")
        
        f.write("\n![Top Incident Locations](markitdown_top_locations.png)\n\n")
        
        # Offense Categories
        f.write("## Incident Types\n\n")
        f.write("The incidents have been categorized as follows:\n\n")
        
        for category, count in sorted(offense_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(df)) * 100
            f.write(f"- **{category}**: {count} incidents ({percentage:.1f}%)\n")
        
        f.write("\n![Offense Categories](markitdown_offense_categories.png)\n\n")
        
        # Safety Analysis
        f.write("## Safety Analysis\n\n")
        
        if safety_df is not None and not safety_df.empty:
            f.write("### Areas with Lower Safety Concerns\n\n")
            f.write("These areas have fewer incidents and less severe types of incidents:\n\n")
            
            for street, row in safety_df.head(10).iterrows():
                f.write(f"- **{street}**: Safety Score: {row['safety_score']:.2f} ({row['incident_count']} incidents)\n")
            
            f.write("\n### Areas with Higher Safety Concerns\n\n")
            f.write("These areas have more incidents or more severe types of incidents:\n\n")
            
            for street, row in safety_df.tail(10).iloc[::-1].iterrows():
                f.write(f"- **{street}**: Safety Score: {row['safety_score']:.2f} ({row['incident_count']} incidents)\n")
            
            f.write("\n![Location Safety Comparison](markitdown_location_safety.png)\n\n")
        
        # Recommendations
        f.write("## Recommendations for House Hunting\n\n")
        
        f.write("### Suggested Areas to Consider\n\n")
        if safety_df is not None and not safety_df.empty:
            safest_streets = safety_df.head(5).index.tolist()
            f.write("Based on our analysis of police reports, these areas may be worth considering for their lower incident rates:\n\n")
            for street in safest_streets:
                f.write(f"- **{street}** area\n")
        else:
            f.write("Insufficient data to make specific area recommendations.\n")
        
        f.write("\n### Areas That May Need More Research\n\n")
        if safety_df is not None and not safety_df.empty:
            concern_streets = safety_df.tail(5).index.tolist()
            f.write("These areas show higher incident rates and may warrant additional research before making housing decisions:\n\n")
            for street in concern_streets:
                f.write(f"- **{street}** area\n")
        else:
            f.write("Insufficient data to identify specific areas of concern.\n")
        
        # Conclusion
        f.write("\n## Conclusion\n\n")
        f.write("This analysis provides a data-driven overview of safety patterns in different Palo Alto neighborhoods based on recent police reports. ")
        f.write("While this information can be valuable for house hunting, it should be used as one of many factors in your decision-making process. ")
        f.write("We recommend complementing this analysis with personal visits to prospective neighborhoods at different times of day and speaking with local residents.\n\n")
        
        f.write("*Note: This analysis was generated using the markitdown tool for extracting text from PDF police reports.*\n")
    
    print(f"Comprehensive report generated at: {report_path}")
    return report_path


if __name__ == "__main__":
    # Load data from CSV
    df = load_data()
    
    if df is not None:
        # Clean and normalize the data
        cleaned_df = clean_data(df)
        
        if cleaned_df is not None:
            # Analyze the data
            stats = analyze_data(cleaned_df)
            
            # Generate visualizations
            if stats:
                visualization_paths = generate_visualizations(cleaned_df, stats)
                
                # Generate comprehensive report
                report_path = generate_comprehensive_report(cleaned_df, stats)
                
                if report_path:
                    print(f"\nAnalysis complete! Report available at: {report_path}")
                    
                    # Print some key findings for quick reference
                    if 'safety_scores' in stats and not stats['safety_scores'].empty:
                        safety_df = stats['safety_scores']
                        print("\nKey Findings:")
                        print("Areas with fewer safety concerns:")
                        for street in safety_df.head(3).index:
                            print(f"  - {street}")
                        
                        print("\nAreas with more safety concerns:")
                        for street in safety_df.tail(3).iloc[::-1].index:
                            print(f"  - {street}")
            else:
                print("Analysis did not generate any results.")