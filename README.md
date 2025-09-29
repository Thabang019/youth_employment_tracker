# Grant Permission - Host machine

mkdir -p ./airflow/data/outputs 
chown -R 50000:50000 ./airflow/data

# Start project
docker compose up -d

# Stop project
docker compose down

# Restart after making changes
docker compose up -d --build

# Connecting - PowerBI Desktop

datasource:PostgreSQL database

server:localhost
database:youth_employment
username:postgres
password:password
