from chakraview.chakraview import ChakraView
from chakraview.config import sessions, lot_sizes, strike_diff
from chakraview.logger import logger
import pandas as pd
import numpy as np
import datetime
from analysis.calculate_metrics import CalculateMetrics


class BTST(ChakraView):

    def __init__(self):
        super().__init__()
        self.signal_list = []
        self.logger = logger
        self.reset_all_variables()
        self.new_day = None
        self.calc = CalculateMetrics()
    
    def check_new_day(self, date: datetime.date) -> bool:
        if date == self.new_day:
            return False
        if date != self.new_day:
            self.new_day = date
            return True
    
    def get_resampled_tick(self, date: datetime.date, time: datetime.time):
        timestamp_offset = self.timeframe - 1
        base_dt = datetime.datetime.combine(date, time)

        if time == datetime.time(15, 5):
            adjusted_timestamp = base_dt + datetime.timedelta(minutes=24)
        else:
            adjusted_timestamp = base_dt + datetime.timedelta(minutes = timestamp_offset)
        
        return adjusted_timestamp.time()
    

    def reset_all_variables(self):
        self.in_position = 0
        self.entry_symbol = None
    
    def set_params_from_uid(self, uid: str):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.timeframe = uid_split.pop(0)
        self.multiplier = int(uid_split.pop(0))
        self.fut_expiry_code = uid_split.pop(0)
        self.expiry_code = int(uid_split.pop(0))
        self.moneyness = int(uid_split.pop(0))
        self.qty = lot_sizes.get(self.underlying)
        self.strike_diff = strike_diff.get(self.underlying)
        self.start = sessions.get(self.underlying).get('start')
        self.end = sessions.get(self.underlying).get('end')
    

    def create_itertuples(self, df: pd.DataFrame):
        return df.itertuples(index=False)
    

    def resample_df(self, db: pd.DataFrame) -> pd.DataFrame:

        df = db[(db['time'] >= self.start) & (db['time'] <= self.end)].copy()

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
    
    def calculate_vwap(self, db: pd.DataFrame) -> pd.DataFrame:
        multiplier = self.multiplier
        """Calculate VWAP per trading session (per day)"""
        db['fair_price'] = (db['h'] + db['l'] + db['c']) / 3
        db['weighted_price'] = db['fair_price'] * db['v']
        # ✅ Per-day cumulative sums
        db['cum_volume'] = db.groupby('date')['v'].cumsum()
        db['cum_weighted_price'] = db.groupby('date')['weighted_price'].cumsum()
        db['vwap'] = db['cum_weighted_price'] / db['cum_volume']
        db['vwap_std'] = db.groupby('date')['vwap'].expanding().std().reset_index(level=0, drop=True)
        db['upperband'] = db['vwap'] + multiplier * db['vwap_std']
        db['lowerband'] = db['vwap'] - multiplier * db['vwap_std']
        return db

    def gen_signals(self, row):
        
        new_day = self.check_new_day(row.date)

        #ENTRY
        if row.time == datetime.time(15, 28):
            if self.in_position == 0:
                if row.c > row.upperband:
                    current_timestamp = f'{row.date} {row.time}'
                    self.logger.info(f'BTST LONG SIGNAL FOUND AT {row.date} {row.time}')
                    entry_tick = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, row.time, row.c, self.strike_diff, 'CE', self.moneyness)
                    if entry_tick:
                        self.entry_symbol = entry_tick.get('symbol')
                        entry_price = entry_tick.get('c')
                        entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'BUY', 'LONG_ENTRY')
                        self.signal_list.append(entry_trade)
                        self.in_position = 1
                    else:
                        self.logger.warning(f"Entry Call Tick Returned None At {row.date} {row.time}")
                
                elif row.c < row.lowerband:
                    current_timestamp = f'{row.date} {row.time}'
                    self.logger.info(f'BTST SHORT SIGNAL FOUND AT {row.date} {row.time}')
                    entry_tick = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, row.time, row.c, self.strike_diff, 'PE', self.moneyness)
                    if entry_tick:
                        self.entry_symbol = entry_tick.get('symbol')
                        entry_price = entry_tick.get('c')
                        entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'BUY', 'LONG_ENTRY')
                        self.signal_list.append(entry_trade)
                        self.in_position = -1
                    else:
                        self.logger.warning(f"Entry Put Tick Returned None At {row.date} {row.time}")
        
        if new_day:
            if row.time == datetime.time(9, 15, 0):
                if self.in_position == 1:
                    current_timestamp = f'{row.date} {row.time}'
                    self.logger.info(f'BTST LONG EXIT SIGNAL FOUND AT {current_timestamp}')
                    exit_tick = self.get_tick(self.entry_symbol, row.date, row.time)
                    if exit_tick:
                        exit_price = exit_tick.get('c')
                        exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'SELL', 'LONG_EXIT')
                        self.signal_list.append(exit_trade)
                        self.reset_all_variables()
                    else:
                        self.logger.warning(f"Exit Call Tick Returned None at {current_timestamp}")
                        self.reset_all_variables()

                elif self.in_position == -1:
                    current_timestamp = f'{row.date} {row.time}'
                    self.logger.info(f'BTST SHORT EXIT SIGNAL FOUND AT {current_timestamp}')
                    exit_tick = self.get_tick(self.entry_symbol, row.date, row.time)
                    if exit_tick:
                        exit_price = exit_tick.get('c')
                        exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'SELL', 'LONG_EXIT')
                        self.signal_list.append(exit_trade)
                        self.reset_all_variables()
                    else:
                        self.logger.warning(f"Exit Put Tick Returned None at {current_timestamp}")
                        self.reset_all_variables()
        
    def run_backtest(self, uid: str):
        self.set_params_from_uid(uid)
        spot_df = self.get_fut_df(self.underlying, self.fut_expiry_code)
        spot_df = spot_df.sort_values(by=['date', 'time']).reset_index(drop=True)
        resampled_df = self.resample_df(spot_df)
        vwap_df = self.calculate_vwap(resampled_df)
        iterable = self.create_itertuples(vwap_df)
        for row in iterable:
            self.gen_signals(row)
        
        tradebook = pd.DataFrame(self.signal_list)
        tradebook = self.calc.calculate_pl_in_opt_tradesheet(tradebook)
        tradebook.to_parquet(f'C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/btst/{uid}.parquet')

