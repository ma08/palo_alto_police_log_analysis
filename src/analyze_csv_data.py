#!/usr/bin/env python3
"""
Analyze police incident CSV data generated from markdown files.
"""

import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime


def load_csv_files(csv_dir="data/csv_files", combined_csv="data/processed/combined_incidents.csv"):
    """
    Load and combine all CSV files.
    
    Args:
        csv_dir: Directory containing CSV files
        combined_csv: Path to save the combined CSV
        
    Returns:
        DataFrame with combined data
    """
    # Find all CSV files
    csv_files = sorted(Path(csv_dir).glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {csv_dir}")
        return None
    
    # Load each CSV file and add source file info
    dfs = []
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            
            # Add source file info
            df['source_file'] = csv_file.name
            
            # Extract date from filename
            date_match = re.search(r'(march|april)-(\d{2})-2025', csv_file.name)
            if date_match:
                month = date_match.group(1)
                day = date_match.group(2)
                month_num = 3 if month.lower() == 'march' else 4
                report_date = f"2025-{month_num:02d}-{day}"
                df['report_date'] = report_date
            
            dfs.append(df)
            print(f"Loaded {len(df)} incidents from {csv_file}")
        except Exception as e:
            print(f"Error loading {csv_file}: {e}")
    
    if not dfs:
        print("No data loaded from CSV files.")
        return None
    
    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"Combined {len(combined_df)} incidents from {len(dfs)} files")
    
    # Save combined data
    os.makedirs(os.path.dirname(combined_csv), exist_ok=True)
    combined_df.to_csv(combined_csv, index=False)
    print(f"Saved combined data to {combined_csv}")
    
    return combined_df


def clean_data(df):
    """
    Clean and normalize the data.
    
    Args:
        df: DataFrame with combined data
        
    Returns:
        Cleaned DataFrame
    """
    if df is None or len(df) == 0:
        print("No data to clean.")
        return None
    
    # Make a copy to avoid SettingWithCopyWarning
    cleaned_df = df.copy()
    
    # Remove duplicates based on case_number
    if 'case_number' in cleaned_df.columns:
        before_count = len(cleaned_df)
        cleaned_df = cleaned_df.drop_duplicates(subset=['case_number'])
        after_count = len(cleaned_df)
        
        if before_count > after_count:
            print(f"Removed {before_count - after_count} duplicate case numbers")
    
    # Normalize date formats
    if 'date' in cleaned_df.columns:
        try:
            cleaned_df['date'] = pd.to_datetime(cleaned_df['date'], errors='coerce')
        except Exception as e:
            print(f"Error normalizing dates: {e}")
    
    # Extract street names from location
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
        'Addison', 'Channing', 'Homer', 'Lytton', 'Everett', 'Park', 'Forest', 'Hanover'
    ]
    
    # Try to match known street names
    for street in streets:
        if re.search(r'\b' + re.escape(street) + r'\b', location, re.IGNORECASE):
            return street
    
    # Extract street name from common patterns
    patterns = [
        r'(\d+)\s+([A-Za-z]+)\s+(St|Ave|Blvd|Rd|Way|Dr|Ln|Ct|Pl|Cir)',  # 123 Main St
        r'([A-Za-z]+)\s+(St|Ave|Blvd|Rd|Way|Dr|Ln|Ct|Pl|Cir)',  # Main St
        r'(\w+)\s*(?:&|/|and)\s*(\w+)',  # Main & First, Main/First
    ]
    
    for pattern in patterns:
        match = re.search(pattern, location, re.IGNORECASE)
        if match:
            groups = match.groups()
            # Return the street name, not the number or type
            if len(groups) == 3:  # Pattern with number, street, type
                return groups[1]
            elif len(groups) == 2:
                if groups[1] in ['St', 'Ave', 'Blvd', 'Rd', 'Way', 'Dr', 'Ln', 'Ct', 'Pl', 'Cir']:
                    return groups[0]  # Return street name
                else:
                    return f"{groups[0]}/{groups[1]}"  # Return intersection
    
    # If no patterns match, return the first word (if multiple) or the whole string
    parts = location.split()
    if len(parts) > 1:
        # Skip numeric parts
        for part in parts:
            if not part.isdigit() and len(part) > 2:  # Skip short parts like "of" or "ST"
                return part
    
    return location if location else None


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
        'Theft': ['theft', 'burglary', 'robbery', 'shoplifting', 'stolen', 'larceny', 'vehicle theft'],
        'Traffic': ['traffic', 'vehicle', 'driving', 'dui', 'parking', 'hit and run', 'accident'],
        'Assault': ['assault', 'battery', 'fight', 'violence', 'domestic', 'rape'],
        'Property Damage': ['vandalism', 'damage', 'graffiti', 'deface'],
        'Drugs/Alcohol': ['drug', 'narcotic', 'alcohol', 'intoxication', 'controlled substance'],
        'Mental Health': ['mental', 'welfare', 'crisis', 'evaluation'],
        'Noise/Disturbance': ['noise', 'disturbance', 'loud', 'party', 'nuisance'],
        'Fraud': ['fraud', 'scam', 'identity theft', 'forgery', 'credit', 'unauth'],
        'Warrant': ['warrant', 'failure to appear']
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
        stats['top_locations'] = street_counts.head(15).to_dict()
    
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
            'Warrant': 3,
            'Other': 2
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
    
    # Time-based analysis - incidents by day of week
    if 'date' in df.columns:
        df_with_date = df.dropna(subset=['date'])
        if not df_with_date.empty:
            try:
                df_with_date['day_of_week'] = df_with_date['date'].dt.day_name()
                day_counts = df_with_date['day_of_week'].value_counts()
                stats['day_of_week_counts'] = day_counts.to_dict()
            except Exception as e:
                print(f"Error calculating day of week: {e}")
    
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
        plt.figure(figsize=(12, 10))
        top_locations = pd.Series(stats['top_locations']).sort_values(ascending=True)
        bars = sns.barplot(x=top_locations.values, y=top_locations.index)
        
        # Add count labels
        for i, v in enumerate(top_locations.values):
            bars.text(v + 0.1, i, str(v), color='black', va='center')
        
        plt.title('Top Streets by Number of Incidents')
        plt.xlabel('Number of Incidents')
        plt.tight_layout()
        
        top_locations_path = os.path.join(output_dir, 'csv_top_locations.png')
        plt.savefig(top_locations_path)
        plt.close()
        visualization_paths.append(top_locations_path)
    
    # 2. Offense categories distribution
    if 'offense_counts' in stats:
        plt.figure(figsize=(10, 8))
        offense_counts = pd.Series(stats['offense_counts'])
        plt.pie(offense_counts, labels=offense_counts.index, autopct='%1.1f%%', 
                startangle=90, shadow=True)
        plt.axis('equal')
        plt.title('Distribution of Offense Categories')
        plt.tight_layout()
        
        categories_path = os.path.join(output_dir, 'csv_offense_categories.png')
        plt.savefig(categories_path)
        plt.close()
        visualization_paths.append(categories_path)
    
    # 3. Location safety scores (lower is better)
    if 'safety_scores' in stats:
        safety_df = stats['safety_scores']
        if not safety_df.empty and len(safety_df) >= 5:
            plt.figure(figsize=(12, 10))
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
            
            safety_path = os.path.join(output_dir, 'csv_location_safety.png')
            plt.savefig(safety_path)
            plt.close()
            visualization_paths.append(safety_path)
    
    # 4. Incidents by day of week
    if 'day_of_week_counts' in stats:
        plt.figure(figsize=(12, 6))
        day_counts = pd.Series(stats['day_of_week_counts'])
        
        # Reorder days of week
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        day_counts = day_counts.reindex(day_order)
        
        # Plot bar chart
        sns.barplot(x=day_counts.index, y=day_counts.values)
        plt.title('Incidents by Day of Week')
        plt.xlabel('Day of Week')
        plt.ylabel('Number of Incidents')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        day_counts_path = os.path.join(output_dir, 'csv_day_of_week.png')
        plt.savefig(day_counts_path)
        plt.close()
        visualization_paths.append(day_counts_path)
    
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
    day_of_week_counts = stats.get('day_of_week_counts', {})
    
    # Create results directory if it doesn't exist
    os.makedirs(results_dir, exist_ok=True)
    
    # Generate report filename
    report_path = os.path.join(results_dir, 'csv_safety_analysis.md')
    
    with open(report_path, 'w') as f:
        # Write report header
        f.write("# Palo Alto Safety Analysis Report (CSV Data)\n\n")
        f.write(f"*Generated on: {datetime.now().strftime('%Y-%m-%d')}*\n\n")
        
        f.write("## Overview\n\n")
        f.write(f"This report analyzes {len(df)} police incidents extracted from Palo Alto Police Department logs ")
        date_range = ""
        if 'date' in df.columns:
            valid_dates = df['date'].dropna()
            if not valid_dates.empty:
                min_date = valid_dates.min()
                max_date = valid_dates.max()
                if pd.notna(min_date) and pd.notna(max_date):
                    date_range = f"from {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}"
        f.write(f"{date_range}. ")
        f.write("The analysis is intended to help identify safer areas for housing in Palo Alto based on police incident reports.\n\n")
        
        # Incident Locations
        f.write("## Incident Locations\n\n")
        f.write("The following streets have the highest number of reported incidents:\n\n")
        
        for street, count in sorted(top_locations.items(), key=lambda x: x[1], reverse=True)[:15]:
            f.write(f"- **{street}**: {count} incidents\n")
        
        f.write("\n![Top Incident Locations](csv_top_locations.png)\n\n")
        
        # Offense Categories
        f.write("## Incident Types\n\n")
        f.write("The incidents have been categorized as follows:\n\n")
        
        total_incidents = sum(offense_counts.values())
        for category, count in sorted(offense_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_incidents) * 100
            f.write(f"- **{category}**: {count} incidents ({percentage:.1f}%)\n")
        
        f.write("\n![Offense Categories](csv_offense_categories.png)\n\n")
        
        # Incidents by Day of Week
        if day_of_week_counts:
            f.write("## Incidents by Day of Week\n\n")
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            f.write("The distribution of incidents across days of the week:\n\n")
            
            for day in day_order:
                if day in day_of_week_counts:
                    count = day_of_week_counts[day]
                    percentage = (count / total_incidents) * 100
                    f.write(f"- **{day}**: {count} incidents ({percentage:.1f}%)\n")
            
            f.write("\n![Incidents by Day of Week](csv_day_of_week.png)\n\n")
        
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
            
            f.write("\n![Location Safety Comparison](csv_location_safety.png)\n\n")
        
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
        
        # Patterns and Trends
        f.write("\n## Additional Patterns and Trends\n\n")
        
        # Offense types by location
        if 'street' in df.columns and 'offense_category' in df.columns:
            top_streets = sorted(top_locations.items(), key=lambda x: x[1], reverse=True)[:5]
            f.write("### Common Incident Types by Location\n\n")
            
            for street, _ in top_streets:
                street_df = df[df['street'] == street]
                if not street_df.empty:
                    street_incidents = street_df['offense_category'].value_counts().head(3)
                    f.write(f"**{street}**:\n")
                    for offense, count in street_incidents.items():
                        f.write(f"- {offense}: {count} incidents\n")
                    f.write("\n")
        
        # Conclusion
        f.write("## Conclusion\n\n")
        f.write("This analysis provides a data-driven overview of safety patterns in different Palo Alto neighborhoods based on recent police reports. ")
        f.write("While this information can be valuable for house hunting, it should be used as one of many factors in your decision-making process. ")
        f.write("We recommend complementing this analysis with personal visits to prospective neighborhoods at different times of day and speaking with local residents.\n\n")
        
        f.write("*Note: This analysis was generated by processing police reports converted to CSV data using advanced language models.*\n")
    
    print(f"Comprehensive report generated at: {report_path}")
    return report_path


if __name__ == "__main__":
    # 1. Load and combine CSV files
    df = load_csv_files()
    
    if df is not None:
        # 2. Clean and normalize the data
        cleaned_df = clean_data(df)
        
        if cleaned_df is not None:
            # 3. Analyze the data
            stats = analyze_data(cleaned_df)
            
            # 4. Generate visualizations
            if stats:
                visualization_paths = generate_visualizations(cleaned_df, stats)
                
                # 5. Generate comprehensive report
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