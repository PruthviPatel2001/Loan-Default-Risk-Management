from kafka import KafkaProducer
import json
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set to INFO to include only relevant logs
logger = logging.getLogger(__name__)

# Adjust Kafka logging level
logging.getLogger('kafka').setLevel(logging.WARNING)  # Reduce Kafka logs to WARNING level

# Set this variable to True to enable test mode
test_mode = True

def produce_messages(file_path, topic, file_type):
    logging.info(f'**** Starting to produce messages from {file_type} file to topic {topic} ****')
    
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
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

if __name__ == '__main__':
    if test_mode:
        # Only load CSV file in test mode
        produce_messages('/Users/pruthvipatel/Documents/projects/Loan Defaulter ETL/Data/application_data_1.csv', 'loan_data_csv', 'csv')
    else:
        # Load CSV, Parquet, and JSON files in non-test mode
        produce_messages('/Users/pruthvipatel/Documents/projects/Loan Defaulter ETL/Data/application_data_1.csv', 'loan_data_csv', 'csv')
        produce_messages('/Users/pruthvipatel/Documents/projects/Loan Defaulter ETL/Data/application_data_1.parquet', 'loan_data_parquet', 'parquet')
        produce_messages('/Users/pruthvipatel/Documents/projects/Loan Defaulter ETL/Data/application_data_1.json', 'loan_data_json', 'json')
