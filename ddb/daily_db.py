from _ddb import Ddb
import os
import pandas as pd
import datetime
from logger import logger

def process_parquets(fp: str):
    df = pd.read_parquet(fp)
    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].astype('float32')
    int_cols = df.select_dtypes(include=['int64']).columns
    df[int_cols] = df[int_cols].astype('int32')
    logger.info(f'Processed Parquet: {fp}')
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

def convert_filename_to_date(filename: str):
    filename_split = filename.split('.')
    date_element = filename_split[0]
    datetime_obj = datetime.datetime.strptime(date_element, '%Y%m%d').date()
    return datetime_obj


if __name__ == '__main__':
    parent_path = r"C:\Users\Admin\Desktop\processed_parquets"
    for parquet in os.listdir(parent_path):
        full_path = os.path.join(parent_path, parquet)
        df = process_parquets(full_path)
        date = convert_filename_to_date(parquet)
        ingest_daily_files(r"C:\Users\Admin\Desktop\db\historical_db.ddb", df, date)
        logger.info(f'Ingested Date In DB: {date}')