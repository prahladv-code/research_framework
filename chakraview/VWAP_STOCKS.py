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
        self.entry_condition_time = datetime.time(9, 15, 0)
        self.exit_condition_time = datetime.time(15, 25, 0)
        self.signal_list = []
        self.calc = CalculateMetrics()
        self.new_day = None
        self.reset_all_variables()
        
    
    def set_params_from_uid(self, uid):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.timeframe = uid_split.pop(0)
        self.supertrend_period = int(uid_split.pop(0))
        self.multiplier = int(uid_split.pop(0))
        
        
    def calculate_vwap(self, df):
        """Calculate VWAP per trading session (per day)"""
        df['fair_price'] = (df['h'] + df['l'] + df['c']) / 3
        df['weighted_price'] = df['fair_price'] * df['v']
        # ✅ Per-day cumulative sums
        df['cum_volume'] = df.groupby('date')['v'].cumsum()
        df['cum_weighted_price'] = df.groupby('date')['weighted_price'].cumsum()
        df['vwap'] = df['cum_weighted_price'] / df['cum_volume']
        
        return df
    
    def calculate_supertrend(self, db):
        """
        Calculates Supertrend across ALL dates in db (for proper lookback continuity).
        db must be sorted by timestamp before calling.
        """
        try:
            df = db.copy()
            
            prev_close = df['c'].shift(1)
            tr = np.maximum.reduce([
                df['h'] - df['l'],
                (df['h'] - prev_close).abs(),
                (df['l'] - prev_close).abs()
            ])
            
            atr = pd.Series(tr).rolling(self.supertrend_period).mean().to_numpy()
            hl2 = ((df['h'] + df['l']) / 2).to_numpy()
            
            basic_upper = hl2 + self.multiplier * atr
            basic_lower = hl2 - self.multiplier * atr
            
            close = df['c'].to_numpy()
            n = len(df)
            
            final_upper = np.full(n, np.nan)
            final_lower = np.full(n, np.nan)
            supertrend = np.full(n, np.nan)
            trend = np.full(n, 0)
            
            valid_indices = np.where(~np.isnan(atr))[0]
            if len(valid_indices) == 0:
                print('Not enough data to calculate Supertrend.')
                return pd.DataFrame()

            start = valid_indices[0]
            
            final_upper[start] = basic_upper[start]
            final_lower[start] = basic_lower[start]
            
            if close[start] <= final_upper[start]:
                trend[start] = -1
                supertrend[start] = final_upper[start]
            else:
                trend[start] = 1
                supertrend[start] = final_lower[start]
            
            for i in range(start + 1, n):
                if basic_upper[i] < final_upper[i-1] or close[i-1] > final_upper[i-1]:
                    final_upper[i] = basic_upper[i]
                else:
                    final_upper[i] = final_upper[i-1]
                
                if basic_lower[i] > final_lower[i-1] or close[i-1] < final_lower[i-1]:
                    final_lower[i] = basic_lower[i]
                else:
                    final_lower[i] = final_lower[i-1]
                
                if trend[i-1] == 1:
                    trend[i] = -1 if close[i] <= final_lower[i] else 1
                else:
                    trend[i] = 1 if close[i] >= final_upper[i] else -1
                
                supertrend[i] = final_lower[i] if trend[i] == 1 else final_upper[i]
            
            df['final_upper'] = final_upper
            df['final_lower'] = final_lower
            df['supertrend'] = supertrend
            df['trend'] = trend
            return df

        except Exception as e:
            print(f'Error In Calculating Supertrend: {e}')
            return pd.DataFrame()

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
                if vwap_row.c > vwap_row.supertrend:
                    if self.in_position == -1:                                
                        current_timestamp = f'{vwap_row.date} {vwap_row.time}'
                        logger.info(f'VWAP SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                        if self.entry_symbol:
                            exit_tick = vwap_row.c
                            exit_signal = self.place_trade(current_timestamp, self.entry_symbol, exit_tick, 5, 5*exit_tick, 'COVER', 'EXIT_SHORT')
                            self.signal_list.append(exit_signal)
                            self.in_position = 0
                        else:
                            logger.warning(f'Symbol Is Empty. Skipping.')
                            self.in_position = 0
                    
                        logger.info(f'VWAP LONG ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                        self.entry_symbol = self.underlying
                        if self.entry_symbol:
                            entry_tick = vwap_row.c
                            entry_signal = self.place_trade(current_timestamp, self.entry_symbol, entry_tick, 5, 5*entry_tick, "BUY", "ENTRY_LONG")
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
                            entry_signal = self.place_trade(current_timestamp, self.entry_symbol, entry_tick, 5, 5*entry_tick, "BUY", "ENTRY_LONG")
                            self.in_position = 1
                            self.signal_list.append(entry_signal)
                        else:
                            logger.warning(f'Symbol Is Empty. Skipping.')
                            self.in_position = 0

                if vwap_row.c < vwap_row.supertrend:
                    if self.in_position == 1:
                        current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                        print(f'VWAP LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                        if self.entry_symbol:
                            exit_tick = vwap_row.c
                            exit_signal = self.place_trade(current_timestamp, self.entry_symbol, exit_tick, 5, 5*exit_tick, 'SELL', 'EXIT_LONG')
                            self.signal_list.append(exit_signal)
                            self.in_position = 0
                        else:
                            print(f'Symbol Is Empty. Skipping.')
                            self.in_position = 0
   
            
            if vwap_row.c < vwap_row.vwap:
                if vwap_row.c < vwap_row.supertrend:
                    if self.in_position == 1:
                        current_timestamp = f'{vwap_row.date} {vwap_row.time}'
                        logger.info(f'VWAP LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                        if self.entry_symbol:
                            exit_tick = vwap_row.c
                            exit_signal = self.place_trade(current_timestamp, self.entry_symbol, exit_tick, 5, 5*exit_tick, 'SELL', 'EXIT_LONG')
                            self.signal_list.append(exit_signal)
                            self.in_position = 0
                        else:
                            logger.warning(f'Symbol Is Empty. Skipping.')
                            self.in_position = 0
                    
                        logger.info(f'VWAP SHORT ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                        self.entry_symbol = self.underlying
                        if self.entry_symbol:
                            entry_tick = vwap_row.c
                            entry_signal = self.place_trade(current_timestamp, self.entry_symbol, entry_tick, 5, 5*entry_tick, "SHORT", "ENTRY_SHORT")
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
                            entry_signal = self.place_trade(current_timestamp, self.entry_symbol, entry_tick, 5, 5*entry_tick, "SHORT", "ENTRY_SHORT")
                            self.in_position = -1
                            
                            self.signal_list.append(entry_signal)
                        else:
                            logger.warning(f'Symbol Is Empty. Skipping.')
                            self.in_position = 0
                
                if vwap_row.c > vwap_row.supertrend:
                    if self.in_position == -1:
                        current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                        print(f'VWAP SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                        if self.entry_symbol:
                            exit_tick = vwap_row.c
                            exit_signal = self.place_trade(current_timestamp, self.entry_symbol, exit_tick, 5, 5*exit_tick, 'COVER', 'EXIT_SHORT')
                            self.signal_list.append(exit_signal)
                            self.in_position = 0
                        else:
                            print(f'Symbol Is Empty. Skipping.')
                            self.in_position = 0

        if vwap_row.time >= self.exit_condition_time:
            current_timestamp = f'{vwap_row.date} {vwap_row.time}'
            if self.in_position == -1:
                logger.info(f'VWAP SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                if self.entry_symbol:
                    exit_tick = vwap_row.c
                    exit_signal = self.place_trade(current_timestamp, self.entry_symbol, exit_tick, 5, 5*exit_tick, 'COVER', 'EXIT_SHORT')
                    self.signal_list.append(exit_signal)
                    self.in_position = 0
                else:
                    logger.warning(f'Symbol Is Empty. Skipping.')
                    self.in_position = 0
            
            if self.in_position == 1:
                logger.info(f'VWAP LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                if self.entry_symbol:
                    exit_tick = vwap_row.c
                    exit_signal = self.place_trade(current_timestamp, self.entry_symbol, exit_tick, 5, 5*exit_tick, 'SELL', 'EXIT_LONG')
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
            supertrend_df = self.calculate_supertrend(vwap_df)
            iterable = self.create_itertuples(supertrend_df)
            for row in iterable:
                self.gen_signals(row)
            
            tradesheet = pd.DataFrame(self.signal_list)
            tradesheet = self.calc.calculate_pl_in_positional_tradesheet(tradesheet)
            tradesheet.to_parquet(f"C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/vwapstocks/{uid}.parquet")
            logger.info(f'+++++++++++++++++++++++++++++++++++++++BACKTEST COMPLETE+++++++++++++++++++++++++++++++++++++++++++')
        except Exception as e:
            logger.error(f'Error In Running Backtest: {e}')
        
