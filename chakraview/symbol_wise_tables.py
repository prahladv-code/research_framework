import duckdb
import datetime
import pandas as pd
import logging


logging.basicConfig(
    filename="TableView.log",   # log file name
    level=logging.DEBUG,        # minimum level to log
    format="%(asctime)s - %(levelname)s - %(message)s"  # log format
)

log = logging.getLogger(__name__)  # create a logger object

db  =duckdb.connect(r"C:\Users\admin\Desktop\DuckDB\nifty_daily.ddb")


def fetch_tables(date):
    date_table = db.execute(f'SELECT * FROM "{date}"').fetch_df()
    return date_table

def create_combined_df(df=None):
    start_date = datetime.date(2020 ,1, 1)
    end_date = datetime.date(2024, 12, 31)
    df_list = []
    while start_date <= end_date:
        try:
            df = fetch_tables(start_date.strftime('%Y-%m-%d'))
            df_list.append(df)
            print(f'Successfully Appended Table For {start_date.strftime('%Y-%m-%d')}')
            log.info(f'Successfully Processesed Table For {start_date.strftime('%Y-%m-%d')}')
        except Exception as e:
            print(f'ERROR: {e}')
            log.error(e)
        start_date = start_date + datetime.timedelta(days=1)

    if df_list:
        master_df = pd.concat(df_list, ignore_index=True)
        # sort by datetime
        master_df = master_df.sort_values("date").reset_index(drop=True)
        return master_df
    else:
        return pd.DataFrame()

def create_symbol_wise_tables():
    master_df = create_combined_df()
    db.close()
    symbol_db = duckdb.connect(r"C:\Users\admin\Desktop\DuckDB\nifty_symbolwise.ddb")
    log.info('Creating Master Groups ####################################################')
    grouped_master = master_df.groupby('symbol')
    log.info('Creating DB Tables: #######################################################')
    for group, df in grouped_master:
        try:
            symbol_db.register('df_view', df)
            symbol_db.execute(
                f"""
                        CREATE TABLE IF NOT EXISTS "{group}" AS
                        SELECT * FROM df_view
                    """
            )
            symbol_db.unregister('df_view')
            log.info(f'Created Table For Symbol --> {group}')
        except Exception as e:
            log.error(e)

create_symbol_wise_tables()