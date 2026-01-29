from _ddb import Ddb
import pandas as pd
import datetime
import time
import os

def concatenate_underlyings(parent_folder):
    dict_of_underlyings = {}
    for child_folder in os.listdir(parent_folder):
        if 'IDX_1MIN' in child_folder:
            child_path = os.path.join(parent_folder, child_folder)
            for file in os.listdir(child_path):
                full_path = os.path.join(child_path, file)
                underlying_name = file.split('.csv')[0]
                df = pd.read_csv(full_path, header=None)
                
                if underlying_name in dict_of_underlyings:
                    dict_of_underlyings[underlying_name].append(df)
                else:
                    dict_of_underlyings[underlying_name] = [df]
    
    if not dict_of_underlyings:
        print("Dict Of Underlyings Is Empty")
        return None

    return dict_of_underlyings

def read_csv_underlyings(fp: str, filename: str):
    df = pd.read_csv(fp)
    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].astype('float32')
    int_cols = df.select_dtypes(include=['int64']).columns
    df[int_cols] = df[int_cols].astype('int32')
    df = df.rename(

        columns={'Date': 'date',
                 'Time': 'time',
                 'Open': 'o',
                 'High': 'h',
                 'Low': 'l', 
                 'Close': 'c'}
    )
    
    filename_split = filename.split('_')
    extraction_1 = filename_split[-1]
    extraction_2 = extraction_1.split('.')[0]
    return df, extraction_2

def concat_all_dfs_in_dict(list_of_dfs: list):
    concatted_df = pd.concat(list_of_dfs, ignore_index=True)
    float_cols = concatted_df.select_dtypes(include=['float64']).columns
    concatted_df[float_cols] = concatted_df[float_cols].astype('float32')
    int_cols = concatted_df.select_dtypes(include=['int64']).columns
    concatted_df[int_cols] = concatted_df[int_cols].astype('int32')
    concatted_df.columns = ['date', 'time', 'o', 'h', 'l', 'c', 'v', 'oi']
    return concatted_df

def ingest_to_ddb(dict_of_underlyings: dict):
    ddb = Ddb(r"C:\Users\Prahlad\Desktop\db\historical_db.ddb")
    for key, value in dict_of_underlyings.items():
        concatted_df = concat_all_dfs_in_dict(value)
        print(concatted_df)
        ddb.process_underlyings(concatted_df, key)

def directly_ingest_to_db(df, db_name: str):
    db = Ddb(r"C:\Users\Prahlad\Desktop\db\historical_db.ddb")
    db.process_underlyings(df, db_name)              

if __name__ == '__main__':

    for file in os.listdir(r"C:\Users\Prahlad\Desktop\Truedata\NSE\IDX"):
        filepath = os.path.join(r"C:\Users\Prahlad\Desktop\Truedata\NSE\IDX", file)
        df, underlying = read_csv_underlyings(filepath, file)
        directly_ingest_to_db(df, underlying)



