import sys
sys.path.insert(0, '/opt/airflow/src')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.etl_pipeline import run_etl_pipeline
from load.database_loader import DatabaseLoader
from transform.data_transformer import transform_data
from extract.csv_extractor import extract_data
from transform.data_cleaner import clean_data

default_args = {
    'owner': 'youth_tracker',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'youth_employment_tracker',
    default_args=default_args,
    description='ETL Pipeline for Youth Employment Tracker',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['education', 'employment', 'analytics']
)

def run_etl():
    success = run_etl_pipeline()
    if not success:
        raise Exception("ETL pipeline failed")

def load_to_warehouse():
    raw_data = extract_data()
    cleaned_data = clean_data(raw_data)
    transformed_data = transform_data(cleaned_data)
    
    loader = DatabaseLoader()
    loader.load_to_warehouse(transformed_data)

def generate_reports():
    loader = DatabaseLoader()
    loader.connect()
    placement_report = loader.execute_query("""
        SELECT cohort_name, region, placement_rate
        FROM mv_placement_rates
        ORDER BY placement_rate DESC
    """)

extract_transform_task = PythonOperator(
    task_id='extract_and_transform',
    python_callable=run_etl,
    dag=dag
)

load_warehouse_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    dag=dag
)

generate_reports_task = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag
)

extract_transform_task >> load_warehouse_task >> generate_reports_task
