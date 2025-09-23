import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Define required columns for each data type based on your actual CSV structure
REQUIRED_COLUMNS = {
    "candidates": ["CandidateID", "FirstName", "LastName", "Gender", "Age", "Email", "PhoneNumber", "TeamID", "CohortID", "ProvinceID", "BranchID", "EnrollmentDate"],
    "cohorts": ["CohortID", "CohortName", "StartDate", "EndDate"],
    "coursera": ["ProgressID", "CandidateID", "CourseName", "DateCompleted"],
    "placements": ["PlacementID", "CandidateID", "CompanyName", "PlacementStatus", "StartDate"],
    "teams": ["TeamID", "TeamName", "CohortID"],
    "provinces": ["ProvinceID", "ProvinceName", "BranchName"],
    "projects": ["ProjectID", "ProjectTitle", "TeamID", "PresentationDate", "EvaluationScore"],
    "scrums": ["ScrumID", "TeamID", "SessionDate", "MentorName"]
}

def validate_csv_data(df, data_type):
    """
    Validate the structure and basic quality of CSV data
    """
    if data_type not in REQUIRED_COLUMNS:
        logger.error(f"Unknown data type: {data_type}")
        return False
    
    # Check required columns
    required_cols = REQUIRED_COLUMNS[data_type]
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        logger.error(f"Missing required columns in {data_type}: {missing_cols}")
        return False
    
    # Check for empty data
    if df.empty:
        logger.warning(f"{data_type} DataFrame is empty")
        return True
    
    # Check for duplicate rows
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        logger.warning(f"Found {duplicates} duplicate rows in {data_type}")
    
    # Check for null values in key columns
    key_columns = required_cols[:2]  # First two columns are usually keys
    for col in key_columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            logger.warning(f"Found {null_count} null values in {col} column of {data_type}")
    
    return True