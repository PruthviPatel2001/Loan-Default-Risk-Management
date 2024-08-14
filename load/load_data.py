import pandas as pd
import os
import sys

# Add the directory containing the 'utils' folder to the path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils')))


from load.utils.db import load_dataframe_to_table
from utils.load_to_csv import load_to_csv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Test mode flag
test_mode = False

def load_to_db(df):
    logging.info('Starting data loading')

    try:
        if test_mode:
            logging.info('Test mode is enabled. Loading data to CSV.')
            load_to_csv('Processdata', 'loaded_data.csv', df)
            logging.info('Data loaded to CSV.')
        else:
            # Split the dataframe into different tables based on the columns
            loan_application_df = df[['SK_ID_CURR', 'TARGET', 'NAME_CONTRACT_TYPE', 'AMT_CREDIT', 'AMT_ANNUITY', 'AMT_GOODS_PRICE']]
            applicant_profile_df = df[['SK_ID_CURR', 'CODE_GENDER', 'FLAG_OWN_CAR', 'FLAG_OWN_REALTY', 'CNT_CHILDREN', 'AMT_INCOME_TOTAL', 'NAME_TYPE_SUITE', 'NAME_INCOME_TYPE', 'NAME_EDUCATION_TYPE', 'NAME_FAMILY_STATUS', 'NAME_HOUSING_TYPE', 'REGION_POPULATION_RELATIVE', 'DAYS_BIRTH', 'DAYS_EMPLOYED', 'DAYS_REGISTRATION', 'DAYS_ID_PUBLISH', 'OCCUPATION_TYPE', 'WEEKDAY_APPR_PROCESS_START', 'HOUR_APPR_PROCESS_START', 'ORGANIZATION_TYPE']]
            credit_details_df = df[['SK_ID_CURR', 'AMT_CREDIT', 'AMT_ANNUITY', 'AMT_GOODS_PRICE']]
            credit_bureau_requests_df = df[['SK_ID_CURR', 'AMT_REQ_CREDIT_BUREAU_YEAR']]

            # Load data into tables
            load_dataframe_to_table(loan_application_df, 'Loan_Application')
            load_dataframe_to_table(applicant_profile_df, 'Applicant_Profile')
            load_dataframe_to_table(credit_details_df, 'Credit_Details')
            load_dataframe_to_table(credit_bureau_requests_df, 'Credit_Bureau_Requests')

            logging.info('Data loaded to database successfully.')

    except Exception as e:
        logging.error(f'Error occurred during loading: {e}')
        raise
