import logging
import logging.config
import yaml
from pathlib import Path
from utils.helpers import get_project_root, ensure_directory_exists

def setup_logging():
    """Setup logging configuration"""
    log_dir = get_project_root() / "logs"
    ensure_directory_exists(log_dir)
    
    # Default basic configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / "etl.log"),
            logging.StreamHandler()
        ]
    )