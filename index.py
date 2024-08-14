from time import sleep
from extract.kafkaproducer import produce_messages
from extract.kafkaconsumer import consume_messages

from transform.transformdata import transform_data
from load.load_data import load_to_db

import pandas as pd
import logging


# produce -> consume -> transform -> load

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Filter out unwanted log levels
class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.INFO
    
logger = logging.getLogger()
for handler in logger.handlers:
    handler.addFilter(InfoFilter())

# Define the ETL pipeline

def etl_pipeline():

    df = consume_messages()
    df = transform_data(df)
    load_to_db(df)

etl_pipeline()