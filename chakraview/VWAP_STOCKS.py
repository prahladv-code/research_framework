from chakraview.chakraview import ChakraView
import pandas as pd
import numpy as np
import datetime
import time
from chakraview.config import sessions
from analysis.calculate_metrics import CalculateMetrics
from chakraview.logger import logger

class VWAP(ChakraView):
    def __init__(self):
        super().__init__()
        self.entry_condition_time = datetime.time(9, 16, 0)
        self.exit_condition_time = datetime.time(15, 27, 0)
        self.signal_list = []
        self.calc = CalculateMetrics()
        self.new_day = None
        self.reset_all_variables()
        
    
    def set_params_from_uid(self, uid):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.timeframe = uid_split.pop(0)
    
    def calculate_vwap(self, df):
        """Calculate VWAP For Iterable DataFrame"""
        df['fair_price'] = (df['h'] + df['l'] + df['c'])/3
        df['total_volume'] = df['v'].cumsum()
        df['weighted_price'] = df['fair_price'] * df['v']
        df['total_weighted_price'] = df['weighted_price'].cumsum()
        df['vwap'] = df['total_weighted_price']/df['total_volume']
        return df
    
    def check_newday(self, date):
        if date == self.new_day:
            return False
        if date != self.new_day:
            self.new_day = date
            return True
    
        
    def reset_all_variables(self):
        self.entry_symbol = None
        self.in_position =  0
    
    def create_itertuples(self, df):
        return df.itertuples(index = False)
    
    def resample_df(self, db):

        df = db.copy()

        # create timestamp
        df['timestamp'] = (
            pd.to_datetime(df['date']) +
            pd.to_timedelta(df['time'].astype(str))
        )

        resampled_list = []

        for date, day_df in df.groupby('date'):

            day_df = day_df.set_index('timestamp')

            resampled = day_df.resample(
                f'{self.timeframe}min',
                label='left',
                closed='left',
                origin='start_day',
                offset='9h15min'
            ).agg({
                'o': 'first',
                'h': 'max',
                'l': 'min',
                'c': 'last',
                'v': 'sum'
            })

            resampled_list.append(resampled)

        resampled_df = pd.concat(resampled_list)

        resampled_df = resampled_df.reset_index(drop=False)
        resampled_df['date'] = resampled_df['timestamp'].dt.date
        resampled_df['time'] = resampled_df['timestamp'].dt.time

        return resampled_df.dropna()

    def gen_signals(self, row):        
        vwap_row = row
        newday = self.check_newday(vwap_row.date)
        if newday:
            self.reset_all_variables()

        if self.entry_condition_time <= vwap_row.time < self.exit_condition_time:
            if vwap_row.c > vwap_row.vwap:
                if self.in_position == -1:                                
                    current_timestamp = f'{vwap_row.date} {vwap_row.time}'
                    logger.info(f'VWAP SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                    if self.entry_symbol:
                        exit_tick = vwap_row.c
                        exit_signal = self.place_trade(current_timestamp, self.entry_symbol, exit_tick, 1, 1*exit_tick, 'COVER', 'EXIT_SHORT')
                        self.signal_list.append(exit_signal)
                        self.in_position = 0
                    else:
                        logger.warning(f'Symbol Is Empty. Skipping.')
                        self.in_position = 0
                
                    logger.info(f'VWAP LONG ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                    self.entry_symbol = self.underlying
                    if self.entry_symbol:
                        entry_tick = vwap_row.c
                        entry_signal = self.place_trade(current_timestamp, self.entry_symbol, entry_tick, 1, 1*entry_tick, "BUY", "ENTRY_LONG")
                        self.in_position = 1
                        self.signal_list.append(entry_signal)
                    else:
                        logger.warning(f'Symbol Is Empty. Skipping.')
                        self.in_position = 0

                elif self.in_position == 0:
                    logger.info(f'VWAP LONG ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                    current_timestamp = f'{vwap_row.date} {vwap_row.time}'
                    self.entry_symbol = self.underlying
                    if self.entry_symbol:
                        entry_tick = vwap_row.c
                        entry_signal = self.place_trade(current_timestamp, self.entry_symbol, entry_tick, 1, 1*entry_tick, "BUY", "ENTRY_LONG")
                        self.in_position = 1
                        self.signal_list.append(entry_signal)
                    else:
                        logger.warning(f'Symbol Is Empty. Skipping.')
                        self.in_position = 0
            
            if vwap_row.c < vwap_row.vwap:
                if self.in_position == 1:
                    current_timestamp = f'{vwap_row.date} {vwap_row.time}'
                    logger.info(f'VWAP LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                    if self.entry_symbol:
                        exit_tick = vwap_row.c
                        exit_signal = self.place_trade(current_timestamp, self.entry_symbol, exit_tick, 1, 1*exit_tick, 'SELL', 'EXIT_LONG')
                        self.signal_list.append(exit_signal)
                        self.in_position = 0
                    else:
                        logger.warning(f'Symbol Is Empty. Skipping.')
                        self.in_position = 0
                
                    logger.info(f'VWAP SHORT ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                    self.entry_symbol = self.underlying
                    if self.entry_symbol:
                        entry_tick = vwap_row.c
                        entry_signal = self.place_trade(current_timestamp, self.entry_symbol, entry_tick, 1, 1*entry_tick, "SHORT", "ENTRY_SHORT")
                        self.in_position = -1
                        
                        self.signal_list.append(entry_signal)
                    else:
                        logger.warning(f'Symbol Is Empty. Skipping.')
                        self.in_position = 0

                elif self.in_position == 0:
                    logger.info(f'VWAP SHORT ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                    current_timestamp = f'{vwap_row.date} {vwap_row.time}'
                    self.entry_symbol = self.underlying
                    if self.entry_symbol:
                        entry_tick = vwap_row.c
                        entry_signal = self.place_trade(current_timestamp, self.entry_symbol, entry_tick, 1, 1*entry_tick, "SHORT", "ENTRY_SHORT")
                        self.in_position = -1
                        
                        self.signal_list.append(entry_signal)
                    else:
                        logger.warning(f'Symbol Is Empty. Skipping.')
                        self.in_position = 0

        if vwap_row.time >= self.exit_condition_time:
            current_timestamp = f'{vwap_row.date} {vwap_row.time}'
            if self.in_position == -1:
                logger.info(f'VWAP SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                if self.entry_symbol:
                    exit_tick = vwap_row.c
                    exit_signal = self.place_trade(current_timestamp, self.entry_symbol, exit_tick, 1, 1*exit_tick, 'COVER', 'EXIT_SHORT')
                    self.signal_list.append(exit_signal)
                    self.in_position = 0
                else:
                    logger.warning(f'Symbol Is Empty. Skipping.')
                    self.in_position = 0
            
            if self.in_position == 1:
                logger.info(f'VWAP LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                if self.entry_symbol:
                    exit_tick = vwap_row.c
                    exit_signal = self.place_trade(current_timestamp, self.entry_symbol, exit_tick, 1, 1*exit_tick, 'SELL', 'EXIT_LONG')
                    self.signal_list.append(exit_signal)
                    self.in_position = 0
                else:
                    logger.warning(f'Symbol Is Empty. Skipping.')
                    self.in_position = 0

    def run_backtest(self, uid: str):
        try:
            self.set_params_from_uid(uid)
            stock_df = self.get_spot_df(self.underlying)
            resampled_df = self.resample_df(stock_df)
            vwap_df = self.calculate_vwap(resampled_df)
            iterable = self.create_itertuples(vwap_df)
            for row in iterable:
                self.gen_signals(row)
            
            tradesheet = pd.DataFrame(self.signal_list)
            tradesheet = self.calc.calculate_pl_in_positional_tradesheet(tradesheet)
            tradesheet.to_parquet(f"C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/vwapstocks/{uid}.parquet")
            logger.info(f'+++++++++++++++++++++++++++++++++++++++BACKTEST COMPLETE+++++++++++++++++++++++++++++++++++++++++++')
        except Exception as e:
            logger.error(f'Error In Running Backtest: {e}')
        
