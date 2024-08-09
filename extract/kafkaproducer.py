from kafka import KafkaProducer
import json
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logging.getLogger('kafka').setLevel(logging.WARNING)

Test_mode = True

def produce_messages():
    logging.info('**** Starting to produce messages ****')

    # Define file paths, topics, and file types
    csv_file_path = '/opt/airflow/Data/application_data_1.csv'
    parquet_file_path = '/opt/airflow/Data/application_data_1.parquet'
    json_file_path = '/opt/airflow/Data/application_data_1.json'
    
    file_paths = [csv_file_path]
    topics = ['loan_data_csv']
    file_types = ['csv']

    if not Test_mode:
        file_paths.extend([parquet_file_path, json_file_path])
        topics.extend(['loan_data_parquet', 'loan_data_json'])
        file_types.extend(['parquet', 'json'])

    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',  # Use the Kafka container name
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        for file_path, topic, file_type in zip(file_paths, topics, file_types):
            logging.info(f'Processing {file_type} file: {file_path} to topic: {topic}')
            
            if file_type == 'csv':
                df = pd.read_csv(file_path)
            elif file_type == 'parquet':
                df = pd.read_parquet(file_path)
            elif file_type == 'json':
                df = pd.read_json(file_path)
            else:
                raise ValueError('Unsupported file type')

            for record in df.to_dict(orient='records'):
                producer.send(topic, record)
            
            producer.flush()
            logging.info(f'Successfully sent messages to topic {topic}')
    
    except Exception as e:
        logging.error(f'Error occurred: {e}')
    finally:
        producer.close()
