import pandas as pd
import numpy as np

def bin_features(df):
    df['AMT_INCOME_TOTAL_BIN'] = pd.cut(df['AMT_INCOME_TOTAL'], bins=[0, 20000, 40000, 60000, 80000, np.inf], labels=['0-20k', '20k-40k', '40k-60k', '60k-80k', '80k+'])
    df['AGE_BIN'] = pd.cut(df['AGE'], bins=[0, 20, 30, 40, 50, 60, 70, np.inf], labels=['0-20', '20-30', '30-40', '40-50', '50-60', '60-70', '70+'])
    return df