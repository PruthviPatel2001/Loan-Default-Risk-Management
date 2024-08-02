from kafka import KafkaConsumer
import json
import pandas as pd
import time
import logging
from transform.transform_data import transform_data
from load.load_data import load_to_db

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set to INFO to include only relevant logs
logger = logging.getLogger(__name__)

# Adjust Kafka logging level
logging.getLogger('kafka').setLevel(logging.WARNING)  # Reduce Kafka logs to WARNING level

# Set this variable to True to test with only loan_data_csv topic, False to process all topics
test_mode = True

def consume_messages(topic, batch_size=10000, consume_duration=60):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='loan_default_group',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    logger.info(f"Starting to consume messages from topic {topic}")

    data = []
    total_consumed = 0  # Variable to keep track of the total number of rows consumed
    
    while True:
        # Poll for messages
        message = consumer.poll(timeout_ms=1000)
        for tp, messages in message.items():
            for msg in messages:
                if test_mode and total_consumed >= 1000:
                    break
                data.append(msg.value)
                total_consumed += 1
                logger.info(f"Consumed row {total_consumed}")  # Log the count of rows consumed
        
            if test_mode and total_consumed >= 1000:
                break
        if test_mode and total_consumed >= 1000:
            break
        
        # Process the batch if we have data
        if data:
            logger.info(f"Processing a batch of {len(data)} messages.")
            process_batch(data)
            data = []  # Clear the data list after processing

        if not test_mode:
            logger.info(f"////**** Waiting for {consume_duration} seconds before starting the next batch.////****")
            time.sleep(consume_duration)

def process_batch(data):
    df = pd.DataFrame(data)
    transformed_df = transform_data(df)
    load_to_db(transformed_df)

if __name__ == '__main__':
    # Define topics based on test_mode
    topics = ['loan_data_csv'] if test_mode else ['loan_data_csv', 'loan_data_parquet', 'loan_data_json']
    for topic in topics:
        consume_messages(topic)
