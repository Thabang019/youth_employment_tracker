import logging
from datetime import datetime
from extract.csv_extractor import extract_data
from transform.data_cleaner import clean_data
from transform.data_transformer import transform_data
from transform.report_generator import create_summary_reports
from load.csv_loader import save_outputs
from utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def run_etl_pipeline():
    """
    Main ETL pipeline orchestrator
    """
    try:
        logger.info("Starting ETL pipeline execution")
        start_time = datetime.now()
        
        # Extract phase
        logger.info("Extracting data from CSV files")
        raw_data = extract_data()
        
        if not raw_data:
            logger.error("No data extracted. Check if CSV files exist in data/raw/")
            return False
        
        # Transform phase - Clean and process data
        logger.info("Cleaning extracted data")
        cleaned_data = clean_data(raw_data)
        
        logger.info("Transforming data into business insights")
        transformed_data = transform_data(cleaned_data)
        
        logger.info("Creating summary reports")
        reports = create_summary_reports(transformed_data)
        
        # Load phase - Save processed data
        logger.info("Saving output files")
        save_outputs(transformed_data, reports)
        
        # Calculate execution time
        execution_time = datetime.now() - start_time
        logger.info(f"ETL pipeline completed successfully in {execution_time}")
        
        return True
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}", exc_info=True)
        return False