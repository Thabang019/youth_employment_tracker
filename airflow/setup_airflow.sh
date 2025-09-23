#!/bin/bash
set -e

echo "Creating necessary directories..."
mkdir -p /opt/airflow/data/outputs
mkdir -p /opt/airflow/logs
mkdir -p /opt/airflow/dags
echo "Directories created successfully"

echo "Waiting for database to be ready..."
while ! airflow db check; do
  echo "Database not ready, waiting..."
  sleep 5
done

echo "Initializing Airflow database..."
airflow db migrate

echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || echo "User already exists"

echo "Starting Airflow scheduler in background..."
airflow scheduler &   # run scheduler in background

echo "Starting Airflow webserver..."
exec airflow webserver