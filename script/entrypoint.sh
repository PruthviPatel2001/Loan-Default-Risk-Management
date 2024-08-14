#!/bin/bash
set -e

# Upgrade pip
pip install --upgrade pip

# Install dependencies
if [ -e "/opt/airflow/requirements.txt" ]; then
  pip install -r /opt/airflow/requirements.txt
fi

# Initialize the Airflow database if not already initialized
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Upgrade the database schema
airflow db upgrade

# # Start the Airflow webserver
# exec airflow webserver
