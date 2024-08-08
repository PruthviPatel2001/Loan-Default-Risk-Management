import pandas as pd

def handel_duplicate(df):
    df = df.drop_duplicates(keep='first')
    return df