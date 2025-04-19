#!/usr/bin/env python3
"""
analyze_data.py - Analyzes extracted police report data and generates visualizations.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import re

# Set up paths
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
INPUT_CSV = os.path.join(PROCESSED_DATA_DIR, "police_reports.csv")

def ensure_results_dir_exists():
    """Ensure the results directory exists."""
    os.makedirs(RESULTS_DIR, exist_ok=True)

def load_data():
    """Load the processed police report data."""
    try:
        df = pd.read_csv(INPUT_CSV)
        print(f"Loaded {len(df)} records from {INPUT_CSV}")
        return df
    except FileNotFoundError:
        print(f"Error: {INPUT_CSV} not found. Please run extract_data.py first.")
        return None
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def clean_data(df):
    """Clean and prepare data for analysis."""
    if df is None or df.empty:
        return None
        
    # Convert date columns to datetime if they exist
    date_columns = [col for col in df.columns if 'DATE' in col.upper()]
    for col in date_columns:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        except:
            pass  # Skip if conversion fails
    
    # Fill missing values in key columns
    for col in ['STREET_NAME', 'OFFENSE_CATEGORY']:
        if col in df.columns:
            df[col] = df[col].fillna('Unknown')
    
    # Create a combined time-of-day column if time exists
    if 'TIME' in df.columns:
        def categorize_time(time_str):
            if not isinstance(time_str, str):
                return 'Unknown'
            
            # Try to extract hour from time string
            hour_match = re.search(r'(\d{1,2}):', str(time_str))
            if hour_match:
                try:
                    hour = int(hour_match.group(1))
                    # Check for AM/PM
                    is_pm = 'PM' in str(time_str).upper() or 'P.M' in str(time_str).upper()
                    
                    if is_pm and hour < 12:
                        hour += 12
                    
                    if hour >= 5 and hour < 12:
                        return 'Morning'
                    elif hour >= 12 and hour < 17:
                        return 'Afternoon'
                    elif hour >= 17 and hour < 22:
                        return 'Evening'
                    else:
                        return 'Night'
                except:
                    return 'Unknown'
            return 'Unknown'
        
        df['TIME_OF_DAY'] = df['TIME'].apply(categorize_time)
    
    return df

def analyze_crime_by_location(df):
    """Analyze crime frequency by location."""
    print("\n--- Crime Frequency by Location ---\n")
    
    if df is None or df.empty or 'STREET_NAME' not in df.columns:
        print("Unable to analyze crime by location: data not available")
        return
    
    # Count incidents by street name
    location_counts = df['STREET_NAME'].value_counts()
    
    # Display the top locations
    print("Top 20 locations by incident count:")
    for street, count in location_counts.head(20).items():
        print(f"{street}: {count} incidents")
    
    # Create visualization
    plt.figure(figsize=(12, 8))
    location_counts.head(15).plot(kind='barh')
    plt.title('Top 15 Locations by Incident Count')
    plt.xlabel('Number of Incidents')
    plt.ylabel('Location')
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'top_locations.png'))
    plt.close()
    
    # Save top locations to CSV
    top_locations_df = pd.DataFrame({
        'Location': location_counts.index[:30],
        'Incident Count': location_counts.values[:30]
    })
    top_locations_df.to_csv(os.path.join(RESULTS_DIR, 'top_locations.csv'), index=False)
    
    return location_counts

def analyze_crime_by_type(df):
    """Analyze crime frequency by offense type."""
    print("\n--- Crime Frequency by Type ---\n")
    
    if df is None or df.empty or 'OFFENSE_CATEGORY' not in df.columns:
        print("Unable to analyze crime by type: data not available")
        return
    
    # Count incidents by offense category
    offense_counts = df['OFFENSE_CATEGORY'].value_counts()
    
    # Display the offense categories
    print("Incidents by offense category:")
    for offense, count in offense_counts.items():
        print(f"{offense}: {count} incidents")
    
    # Create visualization
    plt.figure(figsize=(10, 6))
    offense_counts.plot(kind='bar')
    plt.title('Incidents by Offense Category')
    plt.xlabel('Offense Category')
    plt.ylabel('Number of Incidents')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'offense_categories.png'))
    plt.close()
    
    # Save offense categories to CSV
    offense_df = pd.DataFrame({
        'Offense Category': offense_counts.index,
        'Incident Count': offense_counts.values
    })
    offense_df.to_csv(os.path.join(RESULTS_DIR, 'offense_categories.csv'), index=False)
    
    return offense_counts

def analyze_location_safety(df, location_counts, offense_counts):
    """Generate a safety score for each location based on incident frequency and severity."""
    print("\n--- Location Safety Analysis ---\n")
    
    if df is None or df.empty or 'STREET_NAME' not in df.columns or 'OFFENSE_CATEGORY' not in df.columns:
        print("Unable to analyze location safety: data not available")
        return
    
    # Define severity weights for different offense types
    severity_weights = {
        'Assault': 5.0,
        'Theft': 3.0,
        'Drugs': 3.0,
        'DUI/Alcohol': 2.0,
        'Vandalism': 1.5,
        'Traffic': 1.0,
        'Mental Health': 1.0,
        'Disturbance': 1.0,
        'Other': 1.0,
        'Unknown': 0.5
    }
    
    # Create a location-offense matrix
    location_offense_counts = df.groupby(['STREET_NAME', 'OFFENSE_CATEGORY']).size().unstack(fill_value=0)
    
    # Calculate weighted incident scores
    safety_scores = {}
    for location in location_counts.index:
        if location in location_offense_counts.index:
            offense_profile = location_offense_counts.loc[location]
            weighted_sum = sum(offense_profile[cat] * severity_weights.get(cat, 1.0) 
                               for cat in offense_profile.index if cat in severity_weights)
            incident_count = location_counts[location]
            
            # Calculate a safety score (lower is better)
            # Normalize by log of incident count to reduce impact of high frequency
            safety_scores[location] = weighted_sum * (1 + (incident_count / location_counts.max()) * 0.5)
    
    # Convert to DataFrame for easier handling
    safety_df = pd.DataFrame({
        'Location': list(safety_scores.keys()),
        'Safety Score': list(safety_scores.values()),
        'Incident Count': [location_counts.get(loc, 0) for loc in safety_scores.keys()]
    })
    
    # Sort by safety score (higher score = less safe)
    safety_df = safety_df.sort_values('Safety Score', ascending=False)
    
    # Display the results
    print("Locations ranked by safety concerns (higher score = more concerns):")
    for i, (_, row) in enumerate(safety_df.head(20).iterrows()):
        print(f"{i+1}. {row['Location']}: Safety Score {row['Safety Score']:.2f}, {row['Incident Count']} incidents")
    
    # Create visualization
    plt.figure(figsize=(12, 8))
    sns.scatterplot(data=safety_df.head(30), x='Incident Count', y='Safety Score')
    
    # Add location labels to the points
    for i, row in safety_df.head(30).iterrows():
        plt.text(row['Incident Count'], row['Safety Score'], row['Location'], fontsize=8)
    
    plt.title('Location Safety Concerns')
    plt.xlabel('Number of Incidents')
    plt.ylabel('Safety Concern Score (higher = more concerns)')
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'location_safety.png'))
    plt.close()
    
    # Save safety scores to CSV
    safety_df.to_csv(os.path.join(RESULTS_DIR, 'location_safety_scores.csv'), index=False)
    
    return safety_df

def analyze_crime_by_time(df):
    """Analyze crime patterns by time of day."""
    print("\n--- Crime Patterns by Time of Day ---\n")
    
    if df is None or df.empty or 'TIME_OF_DAY' not in df.columns:
        print("Unable to analyze crime by time: time data not available")
        return
    
    # Count incidents by time of day
    time_counts = df['TIME_OF_DAY'].value_counts()
    
    # Display the results
    print("Incidents by time of day:")
    for time_period, count in time_counts.items():
        print(f"{time_period}: {count} incidents")
    
    # Create visualization
    plt.figure(figsize=(10, 6))
    time_counts.plot(kind='bar')
    plt.title('Incidents by Time of Day')
    plt.xlabel('Time of Day')
    plt.ylabel('Number of Incidents')
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'time_of_day.png'))
    plt.close()
    
    # Analyze crime types by time of day
    if 'OFFENSE_CATEGORY' in df.columns:
        time_offense_counts = df.groupby(['TIME_OF_DAY', 'OFFENSE_CATEGORY']).size().unstack(fill_value=0)
        
        # Create visualization
        plt.figure(figsize=(12, 8))
        time_offense_counts.plot(kind='bar', stacked=True)
        plt.title('Offense Types by Time of Day')
        plt.xlabel('Time of Day')
        plt.ylabel('Number of Incidents')
        plt.legend(title='Offense Category', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, 'time_offense_patterns.png'))
        plt.close()
        
        # Save time patterns to CSV
        time_offense_counts.reset_index().to_csv(os.path.join(RESULTS_DIR, 'time_offense_patterns.csv'))

def generate_summary_report(df, location_counts, offense_counts, safety_df):
    """Generate a summary report with recommendations for safer neighborhoods."""
    print("\n--- Generating Summary Report ---\n")
    
    if df is None or df.empty:
        print("Unable to generate report: data not available")
        return
    
    # Get the safest locations (with at least 3 incidents for statistical significance)
    if safety_df is not None:
        min_incidents = 3
        significant_safety_df = safety_df[safety_df['Incident Count'] >= min_incidents]
        safest_locations = significant_safety_df.sort_values('Safety Score').head(10)
        
        # Get the most concerning locations
        concerning_locations = significant_safety_df.sort_values('Safety Score', ascending=False).head(10)
    else:
        safest_locations = pd.DataFrame()
        concerning_locations = pd.DataFrame()
    
    # Generate the report
    report = [
        "# Palo Alto Police Report Analysis - Summary",
        "\n## Overview",
        f"This analysis is based on {len(df)} police incidents reported in Palo Alto over the last 30 days.",
        "\n## Key Findings",
        f"- A total of {len(location_counts)} distinct locations appeared in the reports",
        f"- The most common incident types were: {', '.join(offense_counts.index[:3])}"
    ]
    
    # Add safety recommendations
    report.extend([
        "\n## Safer Neighborhoods",
        "Based on our analysis, these areas had fewer safety concerns:"
    ])
    
    if not safest_locations.empty:
        for i, (_, row) in enumerate(safest_locations.iterrows()):
            report.append(f"{i+1}. **{row['Location']}** - Safety Score: {row['Safety Score']:.2f} ({row['Incident Count']} incidents)")
    else:
        report.append("Insufficient data to determine safer neighborhoods")
    
    # Add areas of concern
    report.extend([
        "\n## Areas with More Safety Concerns",
        "These areas had higher incident rates or more serious offenses:"
    ])
    
    if not concerning_locations.empty:
        for i, (_, row) in enumerate(concerning_locations.iterrows()):
            report.append(f"{i+1}. **{row['Location']}** - Safety Score: {row['Safety Score']:.2f} ({row['Incident Count']} incidents)")
    else:
        report.append("Insufficient data to determine concerning areas")
    
    # Add timing information
    report.extend([
        "\n## Time-Based Patterns",
        "Consider these time-related safety insights:"
    ])
    
    if 'TIME_OF_DAY' in df.columns:
        time_counts = df['TIME_OF_DAY'].value_counts()
        most_incidents_time = time_counts.index[0]
        least_incidents_time = time_counts.index[-1]
        report.append(f"- Most incidents occur during the **{most_incidents_time}** ({time_counts[most_incidents_time]} incidents)")
        report.append(f"- Fewest incidents occur during the **{least_incidents_time}** ({time_counts[least_incidents_time]} incidents)")
    else:
        report.append("Time-based analysis not available due to missing data")
    
    # Add recommendations
    report.extend([
        "\n## Recommendations",
        "When considering a neighborhood in Palo Alto:",
        "1. **Visit neighborhoods at different times of day** to get a feel for the area",
        "2. **Talk to current residents** about their safety experiences",
        "3. **Consider proximity to public spaces** like parks and commercial areas, which may have different incident patterns",
        "4. **Look at the types of incidents** in each area - some areas may have higher counts of minor issues rather than serious crimes",
        "\n## Data Limitations",
        "- This analysis only includes the last 30 days of police reports",
        "- Some incidents may not be reported or included in these logs",
        "- The safety scoring system is a simplified model and should be used as one of many factors in decision-making"
    ])
    
    # Write the report to a file
    with open(os.path.join(RESULTS_DIR, 'safety_report.md'), 'w') as f:
        f.write('\n'.join(report))
    
    print(f"Summary report saved to {os.path.join(RESULTS_DIR, 'safety_report.md')}")
    
    # Print safest neighborhoods
    print("\nRecommended safer neighborhoods:")
    if not safest_locations.empty:
        for i, (_, row) in enumerate(safest_locations.head(5).iterrows()):
            print(f"{i+1}. {row['Location']} - Safety Score: {row['Safety Score']:.2f}")
    else:
        print("Insufficient data to determine safer neighborhoods")

def main():
    """Main function to analyze police report data."""
    ensure_results_dir_exists()
    
    print("Loading police report data...")
    df = load_data()
    
    if df is None or df.empty:
        print("No data available for analysis. Please run download_reports.py and extract_data.py first.")
        return
    
    print("Cleaning and preparing data...")
    df = clean_data(df)
    
    print("Analyzing crime by location...")
    location_counts = analyze_crime_by_location(df)
    
    print("Analyzing crime by type...")
    offense_counts = analyze_crime_by_type(df)
    
    print("Analyzing location safety...")
    safety_df = analyze_location_safety(df, location_counts, offense_counts)
    
    print("Analyzing crime by time...")
    analyze_crime_by_time(df)
    
    print("Generating summary report...")
    generate_summary_report(df, location_counts, offense_counts, safety_df)
    
    print("\nAnalysis complete. Results saved to", RESULTS_DIR)

if __name__ == "__main__":
    main()