# utils/db_utils.py

from sqlalchemy import create_engine
import pandas as pd
import logging
from sqlalchemy.exc import SQLAlchemyError

# Database connection details
database_name = "loan_management"
username = "root"
password = "pruthvi12"
host = "localhost"
port = "3306"

# Create SQLAlchemy engine
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database_name}')

def load_dataframe_to_table(df, table_name):
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        logging.info(f'Data loaded successfully into {table_name}.')
    except SQLAlchemyError as e:
        logging.error(f'Error occurred while loading data into {table_name}: {e}')
        raise
