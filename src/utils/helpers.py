import yaml
import os
from pathlib import Path

def get_project_root():
    """Get the project root directory"""
    return Path(__file__).parent.parent.parent

def get_data_path(data_type="raw"):
    """Get path to data directory"""
    root = get_project_root()
    return root / "data" / data_type

def ensure_directory_exists(path):
    """Ensure a directory exists, create if it doesn't"""
    path.mkdir(parents=True, exist_ok=True)
    return path

def get_db_connection_string():
    """
    Build and return the PostgreSQL connection string
    """
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "password")
    host = os.getenv("POSTGRES_HOST", "youth_employment_db")
    port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "youth_employment")
    
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"