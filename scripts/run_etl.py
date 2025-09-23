import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from pipeline.etl_pipeline import run_etl_pipeline
from utils.logger import setup_logging

def main():
    """Main function to run the ETL pipeline"""
    setup_logging()
    print("Starting Youth Employment Tracker ETL Pipeline...")
    
    success = run_etl_pipeline()
    
    if success:
        print("ETL pipeline completed successfully!")
        print("Check the outputs in data/outputs/ folder")
    else:
        print("ETL pipeline failed. Check logs/etl.log for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()