import sys
import os

# Add the path to the project root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract.kafkaproducer import produce_messages
from extract.kafkaconsumer import consume_messages
from transform.transformdata import transform_data
from load.load_data import load_to_db
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Filter out unwanted log levels
class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.INFO

logger = logging.getLogger()
for handler in logger.handlers:
    handler.addFilter(InfoFilter())

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 11),  # Adjust to your start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG('loan_default_pipeline_v3',
    default_args=default_args,
    description='ETL pipeline for loan default prediction',
    schedule_interval='@daily') as dag:

    def produce_data_task():
        try:
            logging.info('Starting Kafka Producer')
            produce_messages()
            logging.info('Kafka Producer completed successfully')
        except Exception as e:
            logging.info(f'Error in Kafka Producer: {e}')
            raise

    def consume_and_transform_data_task():
        try:
            logging.info('Starting Kafka Consumer')
            consume_messages()  # This will consume and transform data in batches
            logging.info('Kafka Consumer completed successfully')

        except Exception as e:
            logging.error(f'Error in consume and transform task: {e}')
        raise

        
    def load_data_task():
        try:
            logging.info('Starting data load to database')
            df = pd.read_csv('/opt/airflow/Data/Processdata/transformed_data.csv')
            load_to_db(df)
            logging.info('Data load to database completed successfully')
        except Exception as e:
            logging.info(f'Error in load data task: {e}')
            raise

    # Define tasks
    produce_task = PythonOperator(
        task_id='produce_data',
        python_callable=produce_data_task,
    )

    consume_and_transform_task = PythonOperator(
        task_id='consume_and_transform_data',
        python_callable=consume_and_transform_data_task,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_task,
    )

    # Set task dependencies
    produce_task >> consume_and_transform_task >> load_task
