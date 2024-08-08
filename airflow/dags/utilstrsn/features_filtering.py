import pandas as pd

def features_filtering(df):
    columns_to_keep = [
        'SK_ID_CURR', 'TARGET', 'NAME_CONTRACT_TYPE', 'CODE_GENDER', 
        'FLAG_OWN_CAR', 'FLAG_OWN_REALTY', 'CNT_CHILDREN', 
        'AMT_INCOME_TOTAL', 'AMT_CREDIT', 'AMT_ANNUITY', 
        'AMT_GOODS_PRICE', 'NAME_TYPE_SUITE', 'NAME_INCOME_TYPE', 
        'NAME_EDUCATION_TYPE', 'NAME_FAMILY_STATUS', 
        'NAME_HOUSING_TYPE', 'REGION_POPULATION_RELATIVE', 
        'DAYS_BIRTH', 'DAYS_EMPLOYED', 'DAYS_REGISTRATION', 
        'DAYS_ID_PUBLISH', 'OCCUPATION_TYPE', 
        'WEEKDAY_APPR_PROCESS_START', 'HOUR_APPR_PROCESS_START', 
        'ORGANIZATION_TYPE', 'AMT_REQ_CREDIT_BUREAU_YEAR'
    ]

    filtered_df = df[columns_to_keep]

    return filtered_df
