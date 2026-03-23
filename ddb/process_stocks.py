from ddb._ddb import Ddb
import pandas as pd
import numpy as np
import datetime
import os

def read_csv_stocks(fp: str):
    df = pd.read_csv(fp)
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d %H:%M:%S%z")
    df['time'] = df['date'].dt.time
    df['date'] = df['date'].dt.date
    df = df.sort_values(by=['date', 'time']).reset_index(drop=True)
    return df

def process_data_csvs(df: pd.DataFrame):
    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].astype('float32')
    int_cols = df.select_dtypes(include=['int64']).columns
    df[int_cols] = df[int_cols].astype('int32')
    df = df.rename(

        columns={'date': 'date',
                 'dime': 'time',
                 'open': 'o',
                 'high': 'h',
                 'low': 'l', 
                 'close': 'c',
                 'volume': 'v'}
    )
    return df


if __name__ == '__main__':
    folder_path = r"C:\Users\Admin\Desktop\DataEq_Nifty50"
    db = Ddb(r"C:\Users\Admin\Desktop\db\historical_db.ddb")
    for file in os.listdir(folder_path):
        underlying = file.split('_')[0]
        full_path = os.path.join(folder_path, file)
        df = read_csv_stocks(full_path)
        processed_df = process_data_csvs(df)
        print(processed_df.head())
        db.process_underlyings(processed_df, underlying)
        

