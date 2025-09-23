import logging
import pandas as pd

logger = logging.getLogger(__name__)

def transform_data(cleaned_data):
    """
    Transform cleaned data into business insights
    """
    transformed_data = {}
    
    # Enhanced candidate data with derived metrics
    if 'candidates' in cleaned_data and 'cohorts' in cleaned_data:
        candidates = cleaned_data['candidates'].copy()
        cohorts = cleaned_data['cohorts']
        
        # Merge with cohort information
        candidates = candidates.merge(cohorts, on='CohortID', how='left')
        
        # Calculate age groups
        candidates['AgeGroup'] = pd.cut(candidates['Age'], 
                                      bins=[0, 25, 30, 35, 50], 
                                      labels=['18-25', '26-30', '31-35', '36+'])
        
        # Calculate enrollment duration (if cohort has ended)
        if 'EndDate' in candidates.columns and 'EnrollmentDate' in candidates.columns:
            candidates['EnrollmentDuration'] = (candidates['EndDate'] - candidates['EnrollmentDate']).dt.days
        
        transformed_data['enhanced_candidates'] = candidates
        logger.info("Enhanced candidates data created")
    
    # Placement success metrics
    if 'placements' in cleaned_data and 'candidates' in cleaned_data:
        placements = cleaned_data['placements'].copy()
        candidates = cleaned_data['candidates']
        
        # Merge placement data with candidate info
        placement_analysis = placements.merge(candidates[['CandidateID', 'Age', 'Gender', 'CohortID', 'ProvinceID']], 
                                            on='CandidateID', how='left')
        
        transformed_data['placement_analysis'] = placement_analysis
        logger.info("Placement analysis data created")
    
    # Coursera completion analysis
    if 'coursera' in cleaned_data and 'candidates' in cleaned_data:
        coursera = cleaned_data['coursera'].copy()
        candidates = cleaned_data['candidates']
        
        # Calculate completion rates by candidate demographics
        coursera_analysis = coursera.merge(candidates[['CandidateID', 'Gender', 'Age', 'CohortID']], 
                                         on='CandidateID', how='left')
        
        transformed_data['coursera_analysis'] = coursera_analysis
        logger.info("Coursera analysis data created")
    
    # Team performance metrics (FIXED - handle missing columns)
    if 'teams' in cleaned_data and 'scrums' in cleaned_data and 'projects' in cleaned_data:
        teams = cleaned_data['teams'].copy()
        scrums = cleaned_data['scrums']
        projects = cleaned_data['projects']
        
        # Team activity analysis - handle missing columns gracefully
        scrum_metrics = scrums.groupby('TeamID').agg({
            'ScrumID': 'count'
        }).rename(columns={'ScrumID': 'TotalScrums'})
        
        # Add attendance metrics if the column exists
        if 'AttendanceCount' in scrums.columns:
            attendance_metrics = scrums.groupby('TeamID')['AttendanceCount'].mean()
            scrum_metrics['AvgAttendance'] = attendance_metrics
        
        team_performance = teams.merge(scrum_metrics, on='TeamID', how='left')
        
        # Add project count per team
        project_count = projects.groupby('TeamID').size().reset_index(name='ProjectCount')
        team_performance = team_performance.merge(project_count, on='TeamID', how='left')
        
        transformed_data['team_performance'] = team_performance
        logger.info("Team performance data created")
    
    logger.info(f"Data transformation completed. Created {len(transformed_data)} transformed datasets")
    return transformed_data