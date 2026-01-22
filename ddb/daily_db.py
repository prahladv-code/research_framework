from _ddb import Ddb
import os
import pandas as pd
import datetime

def process_parquets(fp: str):
    df = pd.read_parquet(fp)
    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].astype('float32')
    int_cols = df.select_dtypes(include=['int64']).columns
    df[int_cols] = df[int_cols].astype('int32')
    print(df.dtypes)
    return df

def parse_daily_filenames(filename: str):
    filename_split = filename.split('_')
    lastname = filename_split[-1]
    lastname_split = lastname.split('.')
    date = lastname_split[0]
    datetime_obj = datetime.datetime.strptime(date, '%Y%m%d').date()
    return datetime_obj

def ingest_daily_files(db_path: str, df:pd.DataFrame, db_name: str):
    db = Ddb(db_path)
    db.process_daily_tables(df, db_name)


if __name__ == '__main__':
    parent_path = r"C:\Users\Prahlad\Desktop\NSE_TRUEDATA_SAMPLES"
    for parquet in os.listdir(parent_path):
        full_path = os.path.join(parent_path, parquet)
        df = process_parquets(full_path)
        date = parse_daily_filenames(parquet)
        ingest_daily_files(r"C:\Users\Prahlad\Desktop\NSE_DB\nse_db.ddb", df, date)