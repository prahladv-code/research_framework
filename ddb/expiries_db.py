from _ddb import Ddb
import pandas as pd
import os
import datetime


def read_parquets(fp: str):
    df = pd.read_parquet(fp)
    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].astype('float32')
    int_cols = df.select_dtypes(include=['int64']).columns
    df[int_cols] = df[int_cols].astype('int32')
    return df

def process_expiry_tables(df, db_path):
    db = Ddb(db_path)
    db.process_expiry_tables(df)

if __name__ == '__main__':
    parent_path = r"C:\Users\Prahlad\Desktop\NSE_TRUEDATA_SAMPLES"
    db_path = r"C:\Users\Prahlad\Desktop\NSE_DB\nse_db.ddb"
    for file in os.listdir(parent_path):
        full_path = os.path.join(parent_path, file)
        df = read_parquets(full_path)
        process_expiry_tables(df, db_path)
    
