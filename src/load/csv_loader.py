import pandas as pd
import logging
import json
from pathlib import Path
from utils.helpers import get_data_path, ensure_directory_exists

logger = logging.getLogger(__name__)

def save_outputs(transformed_data, reports):
    """
    Save all outputs to files
    """
    output_path = get_data_path("outputs")
    ensure_directory_exists(output_path)
    
    # Save transformed datasets
    for name, data in transformed_data.items():
        if isinstance(data, pd.DataFrame):
            file_path = output_path / f"{name}.csv"
            data.to_csv(file_path, index=False)
            logger.info(f"Saved {name} to {file_path}")
    
    # Save reports
    for report_name, report_data in reports.items():
        if report_name == 'program_summary':
            # Save as JSON for easy reading
            file_path = output_path / f"{report_name}.json"
            with open(file_path, 'w') as f:
                json.dump(report_data, f, indent=2, default=str)
            logger.info(f"Saved {report_name} to {file_path}")
        elif isinstance(report_data, dict):
            # Save detailed analytics
            for sub_name, sub_data in report_data.items():
                if isinstance(sub_data, pd.DataFrame):
                    file_path = output_path / f"{report_name}_{sub_name}.csv"
                    sub_data.to_csv(file_path, index=True)
                    logger.info(f"Saved {report_name}_{sub_name} to {file_path}")
                elif hasattr(sub_data, 'to_dict'):
                    # Handle pandas Series
                    file_path = output_path / f"{report_name}_{sub_name}.json"
                    with open(file_path, 'w') as f:
                        json.dump(sub_data.to_dict(), f, indent=2)
                    logger.info(f"Saved {report_name}_{sub_name} to {file_path}")
    
    logger.info(f"All outputs saved to {output_path}")