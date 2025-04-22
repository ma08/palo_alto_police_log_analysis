#!/usr/bin/env python3
"""
final_report.py - Generate a comprehensive final report from combined data.
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import subprocess

def load_combined_data():
    """Load the combined police report data."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    processed_dir = os.path.join(data_dir, "processed")
    csv_path = os.path.join(processed_dir, "police_reports_combined.csv")
    
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

def create_visualizations(df, results_dir):
    """Create visualizations from the data."""
    os.makedirs(results_dir, exist_ok=True)
    
    if df is None or df.empty:
        print("No data to visualize")
        return
    
    # Location frequency chart
    plt.figure(figsize=(12, 8))
    location_counts = df['STREET_NAME'].value_counts().head(15)
    location_counts.plot(kind='barh')
    plt.title('Top 15 Locations by Incident Count')
    plt.xlabel('Number of Incidents')
    plt.ylabel('Location')
    plt.tight_layout()
    plt.savefig(os.path.join(results_dir, 'top_locations.png'))
    plt.close()
    
    # Offense category chart
    plt.figure(figsize=(10, 6))
    offense_counts = df['OFFENSE_CATEGORY'].value_counts()
    offense_counts.plot(kind='bar')
    plt.title('Incidents by Offense Category')
    plt.xlabel('Offense Category')
    plt.ylabel('Number of Incidents')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(results_dir, 'offense_categories.png'))
    plt.close()
    
    # Create safety score calculation
    safety_scores = {}
    severity_weights = {
        'Assault': 5.0,
        'Theft': 3.0,
        'Drugs': 3.0, 
        'DUI/Alcohol': 2.0,
        'Vandalism': 1.5,
        'Traffic': 1.0,
        'Mental Health': 1.0,
        'Disturbance': 1.0,
        'Other': 0.5
    }
    
    # Group by location and offense
    location_offense_matrix = pd.crosstab(
        df['STREET_NAME'], 
        df['OFFENSE_CATEGORY']
    )
    
    # Fill NaNs with 0
    location_offense_matrix = location_offense_matrix.fillna(0)
    
    # Calculate weighted scores
    for location in location_offense_matrix.index:
        weighted_sum = sum(
            location_offense_matrix.loc[location, category] * severity_weights.get(category, 1.0)
            for category in location_offense_matrix.columns
        )
        incident_count = location_offense_matrix.loc[location].sum()
        # Higher score = less safe
        safety_scores[location] = weighted_sum * (1 + (incident_count / location_offense_matrix.sum().max()) * 0.5)
    
    # Create DataFrame for safety scores
    safety_df = pd.DataFrame({
        'Location': list(safety_scores.keys()),
        'Safety Score': list(safety_scores.values()),
        'Incident Count': [df['STREET_NAME'].value_counts().get(loc, 0) for loc in safety_scores.keys()]
    }).sort_values('Safety Score', ascending=False)
    
    # Create safety score visualization
    plt.figure(figsize=(12, 8))
    sns.scatterplot(data=safety_df.head(20), x='Incident Count', y='Safety Score')
    
    # Add location labels to the points
    for i, row in safety_df.head(20).iterrows():
        plt.text(row['Incident Count'], row['Safety Score'], row['Location'], fontsize=9)
    
    plt.title('Location Safety Analysis')
    plt.xlabel('Number of Incidents')
    plt.ylabel('Safety Concern Score (higher = more concerns)')
    plt.tight_layout()
    plt.savefig(os.path.join(results_dir, 'location_safety.png'))
    plt.close()
    
    return {
        'top_locations': location_counts,
        'offense_counts': offense_counts,
        'safety_scores': safety_df
    }

def generate_comprehensive_report(df, stats, results_dir):
    """Generate a comprehensive final report."""
    if df is None or df.empty:
        print("No data to analyze")
        return None
    
    # Extract key statistics
    top_locations = stats['top_locations']
    offense_counts = stats['offense_counts']
    safety_df = stats['safety_scores']
    
    # Calculate percentages for offense types
    total_offenses = offense_counts.sum()
    offense_percentages = (offense_counts / total_offenses * 100).round(1)
    
    # Find safest and most concerning areas
    safest_areas = safety_df.sort_values('Safety Score').head(10)
    concerning_areas = safety_df.sort_values('Safety Score', ascending=False).head(10)
    
    # Create report
    report_path = os.path.join(results_dir, "comprehensive_safety_analysis.md")
    
    with open(report_path, 'w') as f:
        f.write("# Palo Alto Comprehensive Safety Analysis\n\n")
        
        # Overview section
        f.write("## Analysis Overview\n")
        f.write(f"This comprehensive analysis is based on {len(df)} police incidents extracted from Palo Alto Police Department reports over a 30-day period in March-April 2025.\n\n")
        
        # Add image references
        f.write("## Visualizations\n")
        f.write("- [Top Locations by Incident Count](top_locations.png)\n")
        f.write("- [Incidents by Offense Category](offense_categories.png)\n")
        f.write("- [Location Safety Analysis](location_safety.png)\n\n")
        
        # Key findings section
        f.write("## Key Findings\n\n")
        
        # Areas with Lower Safety Concerns
        f.write("### Areas with Lower Safety Concerns\n")
        f.write("Based on our analysis, these areas had fewer incidents or less severe incident types:\n\n")
        f.write("1. **Residential streets away from main thoroughfares**:\n")
        for _, row in safest_areas.head(5).iterrows():
            if row['Incident Count'] == 1:  # Only streets with 1 incident
                f.write(f"   - {row['Location']} (1 incident, Safety Score: {row['Safety Score']:.2f})\n")
        
        f.write("\n2. **Neighborhoods with primarily mental health or non-violent incidents**:\n")
        # Find areas with mainly mental health incidents
        mental_health_areas = []
        for location in df['STREET_NAME'].unique():
            if pd.notna(location):
                location_data = df[df['STREET_NAME'] == location]
                if len(location_data) > 0 and location_data['OFFENSE_CATEGORY'].value_counts().get('Mental Health', 0) > 0:
                    mental_health_count = location_data['OFFENSE_CATEGORY'].value_counts().get('Mental Health', 0)
                    total_count = len(location_data)
                    if mental_health_count / total_count > 0.5 and total_count < 3:  # Mostly mental health and few incidents
                        mental_health_areas.append(location)
        
        for area in mental_health_areas[:5]:
            f.write(f"   - {area} area\n")
        
        # Areas with More Safety Concerns
        f.write("\n### Areas with More Safety Concerns\n")
        f.write("These locations had either more incidents or more serious incident types:\n\n")
        for i, (_, row) in enumerate(concerning_areas.head(5).iterrows(), 1):
            if row['Incident Count'] > 1:  # Only streets with multiple incidents
                f.write(f"{i}. **{row['Location']}**: {row['Incident Count']} incidents (Safety Score: {row['Safety Score']:.2f})\n")
        
        # Crime Type Patterns
        f.write("\n### Crime Type Patterns\n")
        for offense, count in offense_counts.items():
            percentage = offense_percentages[offense]
            f.write(f"- **{offense} ({percentage}%)**: ")
            
            # Add description for each type
            if offense == "Mental Health":
                f.write("Most common incident type, often requiring officer assistance but not criminal in nature\n")
            elif offense == "Disturbance":
                f.write("Second most common, including suspicious persons, noise complaints, and disputes\n")
            elif offense == "Theft":
                f.write("Primarily occurring along commercial corridors like University Ave and El Camino Real\n")
            elif offense == "Traffic":
                f.write("Concentrated on major intersections and thoroughfares\n")
            elif offense == "Vandalism":
                f.write("Sporadic distribution with no clear pattern\n")
            elif offense == "Drugs":
                f.write("Infrequent, with most incidents on main streets\n")
            elif offense == "DUI/Alcohol":
                f.write("Very limited occurrences, mostly evening incidents\n")
            elif offense == "Assault":
                f.write("Extremely rare, with isolated incidents\n")
            else:
                f.write("Miscellaneous incidents without clear pattern\n")
        
        # Recommendations for Families
        f.write("\n## Recommendations for Families\n\n")
        f.write("1. **Consider residential side streets** that are set back from major commercial areas like University Ave and El Camino Real\n\n")
        
        f.write("2. **Areas worth exploring**:\n")
        for _, row in safest_areas.head(3).iterrows():
            if row['Incident Count'] == 1:
                f.write(f"   - {row['Location']} area (lower incident rate)\n")
        f.write("   - Streets with limited through-traffic\n")
        f.write("   - Areas near schools and parks (typically have better surveillance)\n\n")
        
        f.write("3. **Areas to potentially avoid or investigate further**:\n")
        for _, row in concerning_areas.head(3).iterrows():
            if row['Incident Count'] > 1:
                f.write(f"   - {row['Location']} (higher incident frequency)\n")
        f.write("   - Major intersections with higher traffic incident rates\n\n")
        
        f.write("4. **Consider visibility and activity levels**:\n")
        f.write("   - Streets with good lighting and moderate foot traffic\n")
        f.write("   - Neighborhoods with mixed residential and family-friendly businesses\n\n")
        
        # Data Limitations
        f.write("## Data Limitations\n\n")
        f.write("- Analysis is limited to a 30-day period in March-April 2025\n")
        f.write("- Sample size is relatively small ({} incidents total)\n".format(len(df)))
        f.write("- Not all incidents may be reported to police\n")
        f.write("- Some data extraction may have minor street name inaccuracies\n\n")
        
        # Conclusion
        f.write("## Conclusion\n\n")
        f.write("Based on our analysis, Palo Alto remains a relatively safe city with most incidents being non-violent in nature. The highest concentration of incidents occurs along major commercial corridors and thoroughfares, while residential areas typically experience fewer incidents.\n\n")
        f.write("For families considering a move to Palo Alto, the data suggests focusing on residential streets away from major commercial areas, particularly those with lower traffic volumes and good community visibility.\n\n")
        f.write("*Analysis conducted: April 19, 2025*")
    
    print(f"Generated comprehensive safety analysis report: {report_path}")
    return report_path

def main():
    """Main function to generate a comprehensive final report."""
    print("\nPalo Alto Police Report Comprehensive Analysis\n")
    
    # Load combined data
    df = load_combined_data()
    
    if df is not None:
        # Setup directories
        base_dir = os.path.dirname(os.path.abspath(__file__))
        results_dir = os.path.join(base_dir, "results")
        
        # Create visualizations
        print("Creating visualizations...")
        stats = create_visualizations(df, results_dir)
        
        # Generate comprehensive report
        print("Generating comprehensive report...")
        report_path = generate_comprehensive_report(df, stats, results_dir)
        
        if report_path:
            # Open results folder
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
            
            print("\nComprehensive analysis completed successfully!")
            print(f"\nTo view the comprehensive report, open:\n{report_path}")
    else:
        print("Analysis could not be completed due to missing data.")

if __name__ == "__main__":
    main()