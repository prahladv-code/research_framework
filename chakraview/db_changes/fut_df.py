import duckdb
import datetime
import pandas as pd

db = duckdb.connect(r'C:\Users\admin\Desktop\DuckDB\nifty_daily.ddb')

start_date = datetime.date(2020, 1, 1)
fut_df_p = pd.DataFrame()
fut_list = []

while start_date <= datetime.date(2024, 12, 31):
    date_str = start_date.strftime('%Y-%m-%d')
    try:
        df = db.execute(f'SELECT * FROM "{date_str}"').fetch_df()
        fut_df = df[df['symbol'] == 'NIFTY-I']
        fut_list.append(fut_df)
        print(fut_df.head())
    except Exception as e:
        print(f"Error In Processing df {date_str}: {e}")
    start_date += datetime.timedelta(days=1)

# Concatenate all once at the end (much faster than repeated appends)
if fut_list:
    fut_df_p = pd.concat(fut_list, ignore_index=True)

# Register and create table
db.register("fut_df", fut_df_p)
db.execute("CREATE TABLE IF NOT EXISTS nifty_fut AS SELECT * FROM fut_df")
