import pandas as pd
# import sqlalchemy
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_to_db(df):
    logging.info('Starting data loading to csv')
    
    try:

        output_dir = "/Users/pruthvipatel/Documents/projects/Loan Defaulter ETL/Data/Processdata"

        # Define the file path
        file_path = os.path.join(output_dir, 'process_data.csv')

        # Check if the CSV file exists
        if os.path.exists(file_path):
          # Read the existing data
          existing_data = pd.read_csv(file_path)
          # Append new data to existing data
          df = pd.concat([existing_data, df], ignore_index=True)
        else:
            pass

        # Save the combined DataFrame to the CSV file
        df.to_csv(file_path, index=False)


        

        logging.info('Data loaded to database csv.')
    
    except Exception as e:
        logging.error(f'Error occurred during loading: {e}')
        raise
