from kafka import KafkaConsumer
import json
import pandas as pd
import time
import logging

import sys
import os

# Add the path to the sys for transformdata module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))



from transform.transformdata import transform_data
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


# def consume_messages():
#     bootstrap_servers = 'kafka:9092'
#     group_id = 'loan_data_group'
#     topics = ['loan_data_csv', 'loan_data_parquet', 'loan_data_json']

#     # Kafka consumer configuration
#     consumer = KafkaConsumer(
#         bootstrap_servers=bootstrap_servers,
#         group_id=group_id,
#         auto_offset_reset='earliest'
#     )
    
#     # Subscribe to topics
#     consumer.subscribe(topics)
#     logger.info(f"Subscribed to topics: {topics}")
    
#     all_data = []
#     total_records = 0
#     total_messages = 0

#     try:
#         logger.info("Starting to consume messages from Kafka topics...")

#         while True:
#             msg = consumer.poll(timeout_ms=1000)  # Adjust timeout as needed
            
#             if not msg:
#                 logger.info("No new messages. Continuing to poll...")
#                 continue

#             for topic_partition, messages in msg.items():
#                 topic = topic_partition.topic  # Extract topic name
#                 for message in messages:
#                     logger.info(f"Received message from topic '{topic}'")
                    
#                     message_value = message.value
#                      # Log a snippet of the message
#                     total_messages += 1
#                     logger.info(f"Message {total_messages} received successfully.")
                    
#                     try:
#                         logger.info(f"Processing message from topic '{topic}'")
#                         # Handle different formats based on topic
#                         if topic == 'loan_data_csv':
#                             print("----------------- inside csv -----------------") 
#                             data = pd.read_csv(io.StringIO(message_value.decode('utf-8')))
#                         elif topic == 'loan_data_parquet':
#                             print("----------------- inside parquet -----------------") 

#                             data = pd.read_parquet(io.BytesIO(message_value))
#                         elif topic == 'loan_data_json':
#                             print("----------------- inside json -----------------") 

#                             data = pd.read_json(io.StringIO(message_value.decode('utf-8')), lines=True)
#                         else:
#                             logger.warning(f"Unknown topic: {topic}. Skipping...")
#                             continue

#                         # Append DataFrame to the list
#                         logger.info(f"going to append data to all_data")
#                         all_data.append(data)
                       
#                         logger.info(f"Data from topic '{topic}' processed successfully.")

#                         # Print status and yield batch if 10,000 records are reached
#                         if total_messages >= 10000:
#                             logger.info(f"Yielding batch of {total_messages} records")
#                             batch_df = pd.concat(all_data, ignore_index=True)
#                             all_data = []
#                             yield batch_df
#                             logger.info("Batch yielded. Pausing for 10 seconds...")
#                             time.sleep(10)  # Pause to avoid overwhelming the downstream processing

#                     except Exception as e:
#                         logger.error(f"Error processing message from topic '{topic}': {e}")

#     except KeyboardInterrupt:
#         logger.info("Interrupted by user. Exiting...")
#     finally:
#         consumer.close()
#         logger.info("Kafka consumer closed.")
    
#     # Concatenate all remaining data into a single DataFrame if any
#     if all_data:
#         df = pd.concat(all_data, ignore_index=True)
#         logger.info("Remaining data concatenated into DataFrame successfully.")
#         yield df
#     else:
#         logger.info("No remaining data to yield.")
#         yield pd.DataFrame()