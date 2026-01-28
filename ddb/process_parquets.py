from data_processing import DataProcessing
import pandas as pd
import os
import time
from multiprocessing import Pool, cpu_count
import datetime

def worker_process(args):
    fp, symbol = args
    data = DataProcessing()  # one per process
    try:
        meta = data.parse_option_symbol(symbol)
        df = pd.read_csv(fp, header=None)
        processed_df = data.process_options_df(df, symbol, meta)
        return processed_df
    except Exception as e:
        print(f'Error in {symbol}: {e}')
        return pd.DataFrame()
    


if __name__ == "__main__":

    # fp = r"C:\Users\Prahlad\Desktop\Truedata_samples\NSE_OPT_1MIN_20250901"
    for fp in os.listdir(r"C:\Users\Prahlad\Desktop\Truedata_samples"):
        if 'NSE_OPT_1MIN' in fp:
            final_fp = os.path.join(r"C:\Users\Prahlad\Desktop\Truedata_samples", fp)
            tasks = []
            start = time.time()
            for file in os.listdir(final_fp):
                if file.endswith(".csv"):
                    symbol = file.replace(".csv", "")
                    tasks.append((os.path.join(final_fp, file), symbol))

            workers = min(10, cpu_count())

            with Pool(processes=workers) as pool:
                dfs = pool.map(worker_process, tasks)

            valid_dfs = [
                            df for df in dfs
                            if df is not None and not df.empty
                        ]

            final_df = pd.concat(valid_dfs, ignore_index=True)
            final_df.to_parquet(f'C:/Users/Prahlad/Desktop/{fp}.parquet')
            end = time.time()

            print(f'Elapsed Time In Processing 1 Day Data {end-start}')
            print(final_df.shape)
    


# print(processed_df.head(10))