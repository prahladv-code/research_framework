from data_processing import DataProcessing
import pandas as pd
import os
import time
from multiprocessing import Pool, cpu_count
from collections import defaultdict
from logger import logger

def worker_process(args):
    fp, symbol = args
    data = DataProcessing()

    try:
        df = pd.read_csv(fp)
        processed_df = data.process_options_df_new(df)
        logger.info(f'Successfully Processed Symbol: {symbol}')
        return processed_df
    except Exception as e:
        logger.error(f"Error in {symbol}: {e}")
        return pd.DataFrame()


def collect_option_files_by_date(root_dir):
    date_dict = defaultdict(list)

    exchanges = ["BSE", "NSE", "MCX"]
    underlyings = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'GOLD', 'CRUDEOIL', 'SILVER', 'SENSEX', 'BANKEX']

    for exchange in exchanges:
        opt_path = os.path.join(root_dir, exchange, "OPT")

        if not os.path.exists(opt_path):
            continue

        for file in os.listdir(opt_path):
            if not file.endswith(".csv"):
                continue

            try:
                symbol_part, date_part = file.replace(".csv", "").split("_")
                fp = os.path.join(opt_path, file)
                if any(symbol_part.startswith(u) for u in underlyings):
                    date_dict[date_part].append((fp, symbol_part))
            except Exception:
                continue

    return date_dict


if __name__ == "__main__":

    root_dir = r"C:\Users\Admin\Desktop\Truedata"

    date_file_dict = collect_option_files_by_date(root_dir)

    workers = min(10, cpu_count())

    for date, tasks in date_file_dict.items():

        start = time.time()

        with Pool(processes=workers) as pool:
            dfs = pool.map(worker_process, tasks)

        valid_dfs = [df for df in dfs if df is not None and not df.empty]

        if not valid_dfs:
            continue

        final_df = pd.concat(valid_dfs, ignore_index=True)

        output_path = f"C:/Users/Admin/Desktop/processed_parquets/{date}.parquet"
        final_df.to_parquet(output_path)

        end = time.time()

        logger.info(f"###########################################Processed date {date}###############################################################")
        logger.info(f"Shape: {final_df.shape}")
        logger.info(f"Elapsed Time: {end-start:.2f} sec")