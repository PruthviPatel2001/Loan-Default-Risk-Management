from kafka import KafkaProducer
import json
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger('kafka').setLevel(logging.WARNING)

def produce_messages():
    # Define file paths and their corresponding topics
    file_paths = [
        ('/opt/airflow/Data/data.json', 'loan_data_json', 'json'),
        ('/opt/airflow/Data/data.csv', 'loan_data_csv', 'csv'),
        ('/opt/airflow/Data/data.parquet', 'loan_data_parquet', 'parquet')
    ]

    # Create a Kafka producer instance
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=16384,  # 16 KB
        linger_ms=5,       # Wait up to 5 ms before sending a batch
        acks='all',        # Ensure all replicas acknowledge the message
        retries=3          # Number of retries on failure
    )

    try:
        for file_path, topic, file_type in file_paths:
            # Check if the file exists
            if os.path.exists(file_path):
                logging.info(f'File exists: {file_path}')
                
                # Preview the first two rows of the file
                if file_type == 'csv':
                    print(pd.read_csv(file_path, nrows=2))
                elif file_type == 'parquet':
                    print(pd.read_parquet(file_path).head(2))
                elif file_type == 'json':
                    try:
                        # Convert JSON to DataFrame
                        df = pd.read_json(file_path, lines=True)  # Assuming JSON is in newline-delimited format
                        print(df.head(2))
                    except ValueError as ve:
                        logging.error(f'ValueError: {ve} while reading JSON file: {file_path}')
                        continue
                    except Exception as e:
                        logging.error(f'Error reading JSON file: {e}')
                        continue
            else:
                logging.error(f'File does not exist: {file_path}')
                continue

            # Process the file and send messages to Kafka
            logging.info(f'Processing {file_type} file: {file_path} and sending to topic: {topic}')
            
            if file_type == 'csv':
                df = pd.read_csv(file_path)
            elif file_type == 'parquet':
                df = pd.read_parquet(file_path)
            elif file_type == 'json':
                try:
                    df = pd.read_json(file_path, lines=True)  # Assuming JSON is in newline-delimited format
                except ValueError as ve:
                    logging.error(f'ValueError: {ve} while processing JSON file: {file_path}')
                    continue
                except Exception as e:
                    logging.error(f'Error processing JSON file: {e}')
                    continue
            else:
                raise ValueError('Unsupported file type')

            # Log the number of records and start batch processing
            num_records = len(df)
            logging.info(f'Found {num_records} records in {file_type} file {file_path}.')

            batch_size = 5000  #  batch size
            num_batches = (num_records + batch_size - 1) // batch_size  # Compute number of batches

            for i in range(num_batches):
                batch_start = i * batch_size
                batch_end = min(batch_start + batch_size, num_records)
                batch_data = df.iloc[batch_start:batch_end].to_dict(orient='records')

                logging.info(f'Processing batch {i+1} of {num_batches} for file {file_path} (records {batch_start} to {batch_end - 1})')
                
                for record in batch_data:
                    try:
                        producer.send(topic, record)
                    except Exception as e:
                        logging.error(f'Error sending message to Kafka: {e}')

                producer.flush()
                logging.info(f'Batch {i+1} of {num_batches} from file {file_path} successfully sent to topic {topic}')

    except Exception as e:
        logging.error(f'Error occurred: {e}')
    finally:
        producer.close()

