from kafka import KafkaConsumer
import json
import pandas as pd
import logging
import os
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set to INFO to include only relevant logs
logger = logging.getLogger(__name__)

# Adjust Kafka logging level
logging.getLogger('kafka').setLevel(logging.WARNING)  # Reduce Kafka logs to WARNING level

# Set this variable to True to test with only loan_data_csv topic, False to process all topics
test_mode = True

def consume_messages(batch_size=10000, consume_duration=60):
   
    topics = ['loan_data_csv'] if test_mode else ['loan_data_csv', 'loan_data_parquet', 'loan_data_json']

    for topic in topics:
        
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
                df = pd.DataFrame(data)
                df.to_csv(f'/path/to/consumed_data_{topic}.csv', index=False)
                return df

            if not test_mode:
                logger.info(f"////**** Waiting for {consume_duration} seconds before starting the next batch.////****")
                time.sleep(consume_duration)
