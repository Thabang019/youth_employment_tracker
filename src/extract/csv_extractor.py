import pandas as pd
import logging
from pathlib import Path
from utils.helpers import get_data_path
from extract.data_validator import validate_csv_data

logger = logging.getLogger(__name__)

def read_csv_file(file_path, data_type):
    """Read a CSV file with robust error handling"""
    try:
        # Try reading with error handling for malformed CSVs
        df = pd.read_csv(file_path, on_bad_lines='skip', encoding='utf-8')
        
        if df.empty:
            logger.warning(f"File {file_path} is empty or could not be read properly")
            # Try alternative method
            df = pd.read_csv(file_path, error_bad_lines=False, warn_bad_lines=True, encoding='utf-8')
        
        logger.info(f"Successfully read {file_path} with {len(df)} rows")
        
        # Validate the data
        if validate_csv_data(df, data_type):
            return df
        else:
            logger.warning(f"Validation failed for {data_type}, using raw data with warnings")
            return df
            
    except Exception as e:
        logger.error(f"Error reading {file_path}: {str(e)}")
        
        # Try alternative reading methods
        try:
            logger.info(f"Trying alternative CSV reading method for {file_path}")
            df = pd.read_csv(file_path, sep=None, engine='python', encoding='utf-8')
            logger.info(f"Alternative method successful: {len(df)} rows")
            return df
        except Exception as e2:
            logger.error(f"All reading methods failed for {file_path}: {str(e2)}")
            raise

def extract_data():
    """
    Extract data from all CSV files in the raw data directory
    """
    data_path = get_data_path("raw")
    data_files = {
        "candidates": "Candidate.csv",
        "cohorts": "Cohort.csv",
        "coursera": "Coursera.csv",
        "placements": "Placement.csv",
        "teams": "Team.csv",
        "provinces": "Province.csv",
        "projects": "Project.csv",
        "scrums": "Scrum.csv"
    }
    
    
    extracted_data = {}
    
    for data_name, filename in data_files.items():
        file_path = Path(data_path) / filename
        if file_path.exists():
            try:
                df = read_csv_file(file_path, data_name)
                extracted_data[data_name] = df
                logger.info(f"Successfully extracted {data_name} with {len(df)} rows")
            except Exception as e:
                logger.error(f"Failed to extract {data_name}: {str(e)}")
                # Create empty DataFrame as fallback
                extracted_data[data_name] = pd.DataFrame()
        else:
            logger.warning(f"File not found: {file_path}")
            # Create empty DataFrame as fallback
            extracted_data[data_name] = pd.DataFrame()
    
    return extracted_data