import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka import KafkaConsumer, TopicPartition
import pandas as pd
import json
import io
import logging
import time

from transform.transformdata import transform_data

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




def consume_messages():
    bootstrap_servers = 'kafka:9092'
    group_id = 'loan_data_group'
    topics = ['loan_data_csv', 'loan_data_parquet', 'loan_data_json']

    # Kafka consumer configuration
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset='earliest'
    )
    
    # Subscribe to topics
    consumer.subscribe(topics)
    logger.info(f"Subscribed to topics: {topics}")
    
    all_data = []
    total_messages = 0
    batch_size = 10000  # Adjust batch size as needed

    try:
        logger.info("Starting to consume messages from Kafka topics...")

        while True:
            msg = consumer.poll(timeout_ms=1000)  # Poll messages from Kafka topics

            if not msg:
                logger.info("No new messages. Continuing to poll...")
                continue

            for topic_partition, messages in msg.items():
                topic = topic_partition.topic  # Extract topic name
                
                for message in messages:
                    message_value = message.value
                    total_messages += 1

                    all_data.append(message_value)
        
                    if total_messages % 1000 == 0:
                        logger.info(f"{total_messages} messages processed so far")
                        logger.info(f"Sending batch of messages to transform_data()")
                        df = pd.DataFrame(all_data)
                        transformed_df = transform_data(df)
                        all_data = []
                        logger.info(f"Data processed successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise  # Re-raise the exception to ensure Airflow handles it properly

    finally:
        if all_data:
            df = pd.DataFrame(all_data)
            transformed_df = transform_data(df)
            logger.info(f"Remaining data processed before exit.")
        consumer.close()
        logger.info("Kafka consumer closed.")

