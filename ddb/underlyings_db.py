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
                

if __name__ == '__main__':
    dict_of_underlyings = concatenate_underlyings(r"C:\Users\Prahlad\Desktop\Truedata_samples")
    ingest_to_ddb(dict_of_underlyings)



