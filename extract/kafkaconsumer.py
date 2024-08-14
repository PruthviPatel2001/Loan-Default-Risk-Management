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
        
                    if total_messages % 1000 == 0:
                        logger.info(f"Data type of message value: {type(message_value)}")
                        logger.info(f"{total_messages} messages processed so far from topic '{topic}'")

                    try:
                        # Handle different formats based on topic
                        if topic == 'loan_data_csv':
                            
                            data = pd.read_csv(io.StringIO(message_value.decode('utf-8')))
                            logger.info(f"Data type of message value: {type(data)}")

                        elif topic == 'loan_data_parquet':
                            data = pd.read_parquet(io.BytesIO(message_value))
                        elif topic == 'loan_data_json':
                            data = pd.read_json(io.StringIO(message_value.decode('utf-8')), lines=True)
                        else:
                            logger.warning(f"Unknown topic: {topic}. Skipping...")
                            continue

                        # Append DataFrame to the list
                        all_data.append(data)

                        # If batch size is reached, transform the data
                        if total_messages >= batch_size:
                            logger.info(f"Processing batch of {total_messages} records")
                            batch_df = pd.concat(all_data, ignore_index=True)
                            transform_data(batch_df)
                            all_data = []  # Reset the list after processing
                            total_messages = 0  # Reset message count

                    except Exception as e:
                        logger.error(f"Error processing message from topic '{topic}': {e}")

    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting...")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")
    
    # Process any remaining data in the batch
    if all_data:
        logger.info("Processing remaining data")
        batch_df = pd.concat(all_data, ignore_index=True)
        transform_data(batch_df)



