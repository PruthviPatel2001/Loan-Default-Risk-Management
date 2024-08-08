import pandas as pd
import logging
from utilstrsn.binning import bin_features
from utilstrsn.outliers_handling import outliers_handling
from utilstrsn.handel_duplicates import handel_duplicate
from utilstrsn.new_features import  feature_creation_pipeline
from utilstrsn.drop_null import drop_null
from utilstrsn.features_filtering import features_filtering

from utils.load_to_csv import load_to_csv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_data(df):
    logger.info('Starting data transformation')

    try:

        logger.info(f'Initial DataFrame shape: {df.shape}')
        logger.info('Total features in the dataset: {}'.format(df.shape[1]))
        
        df = features_filtering(df)
        logger.info('Filtered features Total features in the dataset: {}'.format(df.shape[1]))

        df = drop_null(df)
        logger.info('Dropped NA values')
        logger.info(f'DataFrame shape after dropping NA: {df.shape}')

        df =  handel_duplicate(df)
        logger.info('Dropped duplicates')

        df =  outliers_handling(df)
        logger.info('Outliers handled')

        df = feature_creation_pipeline(df)
        logger.info('Created new features')

        df = bin_features(df)
        logger.info('Binned features')

        df = load_to_csv('Processdata', 'transformed_data.csv', df)
        logger.info('Transformed Data loaded to CSV')

      
        return df
    
    except Exception as e:
        logging.error(f'Error occurred during transformation: {e}')
        raise
