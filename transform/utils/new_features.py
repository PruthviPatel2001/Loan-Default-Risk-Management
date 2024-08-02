def calculate_debt_to_income_ratio(df):
    df['Debt_to_Income_Ratio'] = df.apply(
        lambda row: (row['AMT_ANNUITY'] + row['AMT_CREDIT']) / row['AMT_INCOME_TOTAL'] if row['AMT_INCOME_TOTAL'] > 0 else 0, 
        axis=1
    )
    return df

def calculate_credit_utilization(df):
    df['CREDIT_UTILIZATION'] = df.apply( 
        lambda row: (row['AMT_CREDIT'] - row['AMT_ANNUITY']) / row['AMT_CREDIT'] if row['AMT_CREDIT'] > 0 else 0,
        axis=1
    )
    return df

def calculate_age(df):
    #  Round to no decimal places
    df['AGE'] = round(-df['DAYS_BIRTH'] / 365.25, 0)
    return df

def calculate_employment_duration(df):
    df['EMPLOYMENT_DURATION'] = -df['DAYS_EMPLOYED'] / 365.25
    return df

def feature_creation_pipeline(df):
    df = calculate_credit_utilization(df)
    df = calculate_debt_to_income_ratio(df)
    df = calculate_age(df)
    return df




