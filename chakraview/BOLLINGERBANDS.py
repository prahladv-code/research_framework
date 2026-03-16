from chakraview.chakraview import ChakraView
import pandas as pd
import numpy as np
import datetime
from config import sessions, lot_sizes, strike_diff

class BOLLINGER(ChakraView):
    def __init__(self):
        super().__init__()
        self.reset_all_variables()
        self.commodities_underlyings = ["GOLD", "CRUDEOIL"]
    
    def reset_all_variables(self):
        self.entry_symbol = None
        self.target_price = None
        self.stoploss_level = None
        self.high_level = None
        self.low_level = None
        self.expiry_date = None
    
    def set_params_from_uid(self, uid: str):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.expiry_code = uid_split.pop(0)
        self.timeframe = int(uid_split.pop(0))
        self.ma_period = int(uid_split.pop(0))
        self.std_multiplier = int(uid_split.pop(0))
        self.target = float(uid_split.pop(0))
    
    def _create_itertuples(self, db):
        return db.itertuples(index = False)
    
    def resample_df(self, db):

        resampled_df = db.copy()

        # create timestamp
        resampled_df['timestamp'] = (
            pd.to_datetime(resampled_df['date']) +
            pd.to_timedelta(resampled_df['time'].astype(str))
        )

        resampled_df.set_index('timestamp', inplace=True)

        resampled_df = resampled_df.resample(
            '5min',
            label='right',
            closed='right'
        ).agg({
            'o': 'first',
            'h': 'max',
            'l': 'min',
            'c': 'last'
        })

        resampled_df = resampled_df.reset_index(drop = False)
        return resampled_df.dropna()
    
    def calculate_bollinger_bands(self, db):
        db['ma'] = db['c'].rolling(self.ma_period).mean()
        db['std'] = db['c'].rolling(self.ma_period).std()
        db['bbandtop'] = db['ma'] + self.std_multiplier * db['std']
        db['bbandbot'] = db['ma'] - self.std_multiplier * db['std']
        return db
    
    def get_resampled_options_tick(self, symbol: str, date: datetime.date, time: datetime.time):
        timestamp_offset = self.timeframe - 1
        adjusted_timestamp = time + datetime.timedelta(minutes=timestamp_offset)
        tick = self.get_tick(symbol, date, adjusted_timestamp)
        if tick:
            return tick
        else:
            return {}

    def gen_signals(self):
        pass
    


