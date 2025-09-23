import pandas as pd
import logging
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def generate_email_from_names(firstname, lastname):
    """Generate email from separate firstname and lastname in format firstname.lastname@capaciti.org.za"""
    try:
        # Clean and process firstname
        if pd.isna(firstname) or str(firstname).strip() == '':
            firstname = 'unknown'
        else:
            firstname = str(firstname).strip().lower()
            firstname = ''.join(e for e in firstname if e.isalnum())
        
        # Clean and process lastname
        if pd.isna(lastname) or str(lastname).strip() == '':
            lastname = 'user'
        else:
            lastname = str(lastname).strip().lower()
            lastname = ''.join(e for e in lastname if e.isalnum())
        
        return f"{firstname}.{lastname}@capaciti.org.za"
    except:
        return "unknown.user@capaciti.org.za"

def clean_candidates_data(df):
    """Clean candidates data with your specific column names"""
    # Remove duplicates
    df = df.drop_duplicates(subset=['CandidateID'])
    
    # Handle missing values
    if 'Age' in df.columns:
        df['Age'] = df['Age'].fillna(df['Age'].median())
    if 'Gender' in df.columns:
        df['Gender'] = df['Gender'].fillna('Unknown')
    if 'PhoneNumber' in df.columns:
        df['PhoneNumber'] = df['PhoneNumber'].fillna('Not Specified')
    
    # Generate emails if missing or invalid
    if 'Email' in df.columns:
        # Fill missing emails with generated ones
        missing_emails = df['Email'].isnull() | (df['Email'] == '')
        if missing_emails.any():
            df.loc[missing_emails, 'Email'] = df.loc[missing_emails].apply(
                lambda row: generate_email_from_names(row['FirstName'], row['LastName']), axis=1
            )
            logger.info(f"Generated {missing_emails.sum()} emails for missing/invalid emails")
        
        # Also check for emails that don't follow the pattern and regenerate them
        invalid_email_pattern = ~df['Email'].str.contains(r'@capaciti\.org\.za$', na=False)
        if invalid_email_pattern.any():
            df.loc[invalid_email_pattern, 'Email'] = df.loc[invalid_email_pattern].apply(
                lambda row: generate_email_from_names(row['FirstName'], row['LastName']), axis=1
            )
            logger.info(f"Regenerated {invalid_email_pattern.sum()} emails with incorrect domain")
    else:
        # Create Email column if it doesn't exist
        df['Email'] = df.apply(lambda row: generate_email_from_names(row['FirstName'], row['LastName']), axis=1)
        logger.info(f"Created Email column with generated emails from FirstName/LastName columns")
    
    # Convert date column
    if 'EnrollmentDate' in df.columns:
        df['EnrollmentDate'] = pd.to_datetime(df['EnrollmentDate'], errors='coerce')
    
    # Ensure proper data types
    df['CandidateID'] = df['CandidateID'].astype(str)
    if 'TeamID' in df.columns:
        df['TeamID'] = df['TeamID'].astype(str)
    if 'CohortID' in df.columns:
        df['CohortID'] = df['CohortID'].astype(str)
    if 'ProvinceID' in df.columns:
        df['ProvinceID'] = df['ProvinceID'].astype(str)
    if 'BranchID' in df.columns:
        df['BranchID'] = df['BranchID'].astype(str)
    
    return df

def clean_cohorts_data(df):
    """Clean cohorts data"""
    df = df.drop_duplicates(subset=['CohortID'])
    
    # Convert date columns if they exist
    if 'StartDate' in df.columns:
        df['StartDate'] = pd.to_datetime(df['StartDate'], errors='coerce')
    if 'EndDate' in df.columns:
        df['EndDate'] = pd.to_datetime(df['EndDate'], errors='coerce')
    
    # Handle missing values
    if 'CohortName' in df.columns:
        df['CohortName'] = df['CohortName'].fillna('Digital Associate')
    
    return df

def clean_coursera_data(df):
    """Clean coursera data"""
    df = df.drop_duplicates(subset=['ProgressID'])
    
    # Convert date column if it exists
    if 'DateCompleted' in df.columns:
        df['DateCompleted'] = pd.to_datetime(df['DateCompleted'], errors='coerce')
    
    return df

def clean_placements_data(df):
    """Clean placements data"""
    df = df.drop_duplicates(subset=['PlacementID'])
    
    # Convert date columns
    if 'StartDate' in df.columns:
        df['StartDate'] = pd.to_datetime(df['StartDate'], errors='coerce')
    if 'EndDate' in df.columns:
        df['EndDate'] = pd.to_datetime(df['EndDate'], errors='coerce')
    
    # Remove records with invalid essential data
    essential_cols = ['PlacementID', 'CandidateID', 'PlacementStatus', 'CompanyName', 'StartDate']
    available_cols = [col for col in essential_cols if col in df.columns]
    
    if available_cols:
        df = df.dropna(subset=available_cols)

    return df

def clean_teams_data(df):
    """Clean teams data"""
    df = df.drop_duplicates(subset=['TeamID'])
    return df

def clean_provinces_data(df):
    """Clean provinces data"""
    df = df.drop_duplicates(subset=['ProvinceID'])
    return df

def clean_projects_data(df):
    """Clean projects data"""
    df = df.drop_duplicates(subset=['ProjectID'])
    return df

def clean_scrums_data(df):
    """Clean scrums data"""
    df = df.drop_duplicates(subset=['ScrumID'])
    # Convert date column if it exists
    if 'SessionDate' in df.columns:
        df['SessionDate'] = pd.to_datetime(df['SessionDate'], errors='coerce')

    return df

def clean_data(raw_data):
    """
    Clean all extracted data
    """
    cleaned_data = {}
    
    cleaning_functions = {
        'candidates': clean_candidates_data,
        'cohorts': clean_cohorts_data,
        'coursera': clean_coursera_data,
        'placements': clean_placements_data,
        'teams': clean_teams_data,
        'provinces': clean_provinces_data,
        'projects': clean_projects_data,
        'scrums': clean_scrums_data
    }
    
    for data_name, df in raw_data.items():
        if data_name in cleaning_functions:
            logger.info(f"Cleaning {data_name} data...")
            cleaned_data[data_name] = cleaning_functions[data_name](df)
            logger.info(f"Cleaned {len(cleaned_data[data_name])} rows in {data_name}")
        else:
            # For any other data, just remove duplicates
            cleaned_data[data_name] = df.drop_duplicates()
            logger.info(f"Basic cleaning done for {data_name}: {len(cleaned_data[data_name])} rows")
    
    logger.info("Data cleaning completed")
    return cleaned_data