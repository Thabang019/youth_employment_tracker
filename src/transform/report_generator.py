import logging
import pandas as pd
import json

logger = logging.getLogger(__name__)

def create_summary_reports(transformed_data):
    """
    Create summary reports for stakeholders
    """
    reports = {}
    
    # Overall program summary
    program_summary = {}
    
    if 'enhanced_candidates' in transformed_data:
        candidates = transformed_data['enhanced_candidates']
        program_summary['total_candidates'] = len(candidates)
        program_summary['gender_distribution'] = candidates['Gender'].value_counts().to_dict()
        if 'AgeGroup' in candidates:
            program_summary['age_distribution'] = candidates['AgeGroup'].value_counts().to_dict()
        program_summary['avg_age'] = candidates['Age'].mean()
    
    if 'placement_analysis' in transformed_data:
        placement_data = transformed_data['placement_analysis']
        total_placements = len(placement_data)
        successful_placements = len(placement_data[
            placement_data['PlacementStatus'].str.contains('Placed|Employed', case=False, na=False)
        ])
        program_summary['placement_rate'] = (successful_placements / total_placements * 100) if total_placements > 0 else 0
        program_summary['total_placements'] = total_placements
        program_summary['successful_placements'] = successful_placements
    
    if 'coursera_analysis' in transformed_data:
        coursera_data = transformed_data['coursera_analysis']
        program_summary['total_course_completions'] = len(coursera_data)
        program_summary['unique_courses'] = coursera_data['CourseName'].nunique()
        
    reports['program_summary'] = program_summary
    
    # Detailed analytics
    if 'placement_analysis' in transformed_data:
        placement_analytics = {}
        placement_data = transformed_data['placement_analysis']
        
        placement_analytics['by_gender'] = placement_data.groupby('Gender')['PlacementStatus'].value_counts().unstack(fill_value=0)
        placement_analytics['by_age_group'] = placement_data.groupby(
            pd.cut(placement_data['Age'], bins=[0, 25, 30, 35, 50], labels=['18-25', '26-30', '31-35', '36+'])
        )['PlacementStatus'].value_counts().unstack(fill_value=0)
        
        if 'ProvinceID' in placement_data:
            placement_analytics['by_province'] = placement_data.groupby('ProvinceID')['PlacementStatus'].value_counts().unstack(fill_value=0)
        
        reports['placement_analytics'] = placement_analytics
    
    if 'coursera_analysis' in transformed_data:
        course_analytics = {}
        coursera_data = transformed_data['coursera_analysis']
        
        course_analytics['completion_by_gender'] = coursera_data.groupby('Gender').size()
        course_analytics['completion_by_course'] = coursera_data.groupby('CourseName').agg({
            'CandidateID': 'count',
        }).round(2)
        
        reports['course_analytics'] = course_analytics
    
    if 'team_performance' in transformed_data:
        reports['team_analytics'] = transformed_data['team_performance']
    
    logger.info(f"Created {len(reports)} summary reports")
    return reports