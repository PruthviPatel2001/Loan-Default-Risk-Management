import numpy as np

def outliers_handling(df):
    # Example using Z-score
    from scipy import stats
    z_scores = stats.zscore(df[['AMT_INCOME_TOTAL', 'AMT_CREDIT', 'AMT_ANNUITY']])
    abs_z_scores = np.abs(z_scores)
    df = df[(abs_z_scores < 3).all(axis=1)]
    return df
