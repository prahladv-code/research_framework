import pandas as pd
import numpy as np
import duckdb
import datetime
import time as t
import logging

class ChakraView:
    def __init__(self):
        self.daily_tb = duckdb.connect(r"C:\Users\admin\Desktop\DuckDB\nifty_daily.ddb", read_only=True)
        logging.basicConfig(filename=r'./ck_logger.log',
                                       level = logging.INFO,
                                       format="%(asctime)s [%(levelname)s] %(message)s")
        self.log = logging.getLogger(self.__class__.__name__)
    
    def get_spot_df(self, spot_name):
        df = self.daily_tb.execute(f"SELECT * FROM {spot_name}").fetch_df()
        df['date'] = pd.to_datetime(df['date']).dt.date
        df = df.rename(columns={    
                                    'open': 'o',
                                    'close': 'c',
                                    'high': 'h',
                                    'low': 'l',
                                    'volume': 'v',
                                    'oi': 'oi'  # optional, just keep as is
                                })
        return df

    def get_df(self, dfname):
        df = self.daily_tb.execute(f"SELECT * FROM {dfname}").fetch_df()
        return df

    def get_tick(self, symbol: str, date: datetime.date, time: datetime.time):
        start = t.time()
        date_str = date.strftime('%Y-%m-%d')
        time_str = time.strftime('%H:%M:%S')
        self.tick_query = f"""
        SELECT * FROM "{date_str}" WHERE time = '{time_str}' AND symbol = '{symbol}'
        """
        df = self.daily_tb.execute(self.tick_query).fetchdf()
        df_renamed = df.rename(columns={'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c', 'volume': 'v'})
        df_filtered = df_renamed[['o', 'h', 'l', 'c', 'v', 'oi']].copy()
        
        if df_filtered.empty:
            return {}
        
        tick_dict = df_filtered.to_dict(orient = 'records')
        end = t.time()
        print(f'Elapsed Time In Getting Tick: {end-start}')
        return tick_dict[0]
    
    def get_all_ticks_by_timestamp(self, date: datetime.date, time: datetime.time):
        start = t.time()
        date_str = date.strftime('%Y-%m-%d')
        time_str = time.strftime('%H:%M:%S')
        self.all_tick_query = f"""
        SELECT * FROM "{date_str}" WHERE time = '{time_str}'
        """
        df = self.daily_tb.execute(self.all_tick_query).fetchdf()
        df_renamed = df.rename(columns={'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c', 'volume': 'v'})
        df_filtered = df_renamed[['symbol', 'o', 'h', 'l', 'c', 'v', 'oi', 'strike', 'right']].copy()
        end = t.time()
        print(f'Elapsed Time In Getting All Ticks: {end-start}')
        return df_filtered
    
    def get_strike_by_moneyness(self, underlying_price, strike_difference, moneyness, right):
        """Calculates Nearest Strike According To Moneyness And Returns An Integer Value."""
        strike = 0
        atm_strike = round(underlying_price/strike_difference) * strike_difference
        if right.upper() == 'CE':
            strike = int(atm_strike + (moneyness*strike_difference))
            return strike
        elif right.upper() == 'PE':
            strike = int(atm_strike - (moneyness*strike_difference))
            return strike
        else:
            raise ValueError("Please Check The Right Input. Valid Values are [CE, PE]")
        

    # def get_all_ticks_by_symbol(self):

    def find_ticker_by_moneyness(self, date, time, underlying_price, strike_difference, right, moneyness):
        start = t.time()
        all_timestamp_df = self.get_all_ticks_by_timestamp(date, time)
        strike = self.get_strike_by_moneyness(underlying_price, strike_difference, moneyness, right)
        moneyness_df = all_timestamp_df[(all_timestamp_df['right'] == right.upper()) & (all_timestamp_df['strike'] == strike)]
        if moneyness_df.empty:
            self.log.error(f'No Data For TimeStamp: {date} {time}')
            return {}
        moneyness_dict = moneyness_df.to_dict(orient='records')
        end=t.time()
        print(f'Elapsed Time In Getting Moneyness Details: {end-start}')
        return moneyness_dict[0]

    def find_ticker_by_premium(self, date, time, underlying_price, right, premium_val, atm_filter=False):
        start = t.time()

        all_timestamp_df = self.get_all_ticks_by_timestamp(date, time)
        option_chain = all_timestamp_df[all_timestamp_df['right'] == right.upper()].copy()

        if option_chain.empty:
            self.log.error(f'No Data For TimeStamp: {date} {time}')
            return {}
        
        option_chain['premium_diff'] = abs(option_chain['c'] - premium_val)
        min_row = option_chain.loc[option_chain['premium_diff'].idxmin()]
        min_row_dict = min_row.to_dict()
        end = t.time()
        print(f'Elapsed Time In Getting Premium Details: {end-start}')
        return min_row_dict
    


