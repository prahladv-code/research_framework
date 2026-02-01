from chakraview.chakraview import ChakraView
import datetime
import pandas as pd
from chakraview.config import sessions, continuous_codes, lot_sizes
import multiprocessing as mp
from analysis.calculate_metrics import CalculateMetrics
import numpy as np
import time

class PRICEMA:
    def __init__(self):
        self.ck = ChakraView()
        self.start_time = sessions.get('nifty_fut').get('start')
        self.end_time = sessions.get('nifty_fut').get('end')
        self.previous_date = None
        self.metrics = CalculateMetrics()
        self.continuous_codes = continuous_codes
    
    def reset_all_variables(self):
        self.entry_price = np.nan
        self.exit_price = np.nan
        self.short_condition = False
        self.long_condition = False
        self.entry_trade = ''
        self.entry_time = None
        self.entry_price = None
        self.position_count = 0
    
    def check_new_day(self, date):
        self.date = date
        if self.date != self.previous_date:
            self.previous_date = self.date
            return True
        elif self.date == self.previous_date:
            return False


    def set_signal_parameters(self, uid):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.expiry_code = uid_split.pop(0)
        self.fut_series = self.continuous_codes.get(self.expiry_code)
        self.ma_period = int(uid_split.pop(0))
        self.timeframe = uid_split.pop(0)
        self.reentry = uid_split.pop(0) == 'True'
        self.qty = lot_sizes.get(self.underlying)
    
    def get_actual_event_timestamp(self, timestamp: datetime):
        timeframe = int(self.timeframe)
        offset = timeframe - 1
        actual_timestamp = timestamp + datetime.timedelta(minutes=offset)
        return actual_timestamp
    
    def create_itertupes(self, db):
        return db.itertuples(index=False)
    
    def generate_uid_from_parameters(self, underlying, expiry_code, ma_period, timeframe, reentry):
        return f'PRICEMA_{underlying}_{expiry_code}_{ma_period}_{timeframe}_{reentry}'


    def generate_resampled_timestamps(self):
        int_timeframe = int(self.timeframe)
        print(f'TimeFrame Debug: {self.timeframe}')
        start_dt = datetime.datetime.combine(datetime.date.today(), self.start_time)
        end_dt = datetime.datetime.combine(datetime.date.today(), self.end_time)
        print(f'Start Time DEBUG: {start_dt}')
        print(f'End Time Debug: {end_dt}')
        adjusted_end_dt = end_dt - datetime.timedelta(minutes=2)
        valid_timestamps = []
        while start_dt <= adjusted_end_dt:
            start_dt += datetime.timedelta(minutes=int_timeframe)

            if start_dt > adjusted_end_dt:
                valid_timestamps.append(adjusted_end_dt.time())
                break
            
            valid_timestamps.append(start_dt.time())

        return valid_timestamps

    def gen_signals(self):
        signals_list = []
        commodities_underlying = None
        if self.underlying in ['GOLD', 'CRUDEOIL']:
            commodities_underlying = self.underlying + '_' + self.fut_series

        if commodities_underlying is not None:
            self.db = self.ck.get_spot_df(commodities_underlying)
        else:
            self.db = self.ck.get_spot_df(self.underlying)
            
        df = self.db.copy()
        df['timestamp'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))

        self.valid_timestamps = self.generate_resampled_timestamps()
        print(f'Valid Timestamp Debug: {self.valid_timestamps}')
        
        resampled_df = df[df['timestamp'].dt.time.isin(self.valid_timestamps)].copy()
        resampled_df.sort_values('timestamp', inplace=True)
        resampled_df['ma'] = resampled_df['c'].rolling(self.ma_period).mean()
        iterable = self.create_itertupes(resampled_df)
        self.in_position = 0
        for self.row in iterable:
            new_day = self.check_new_day(self.row.timestamp.date())
            #ENTRY
            if self.row.timestamp.time() in self.valid_timestamps:
                print(f'Timestamp Match Found: {self.row.timestamp.time()}')
                if self.row.c > self.row.ma:
                    if self.in_position == -1:
                        self.tick = self.ck.get_fut_tick(self.underlying, self.expiry_code, self.row.timestamp.date(), self.row.timestamp.time())
                        self.exit_price = self.tick.get('c') if self.tick else np.nan
                        self.exit_trade = 'COVER'
                        self.entry_price = self.exit_price
                        self.entry_trade = 'BUY'
                        self.in_position = 1
                        signals_list.append(
                                    {
                                        'timestamp': self.row.timestamp,
                                        'symbol': f'{self.underlying}-FUT',
                                        'price': self.exit_price,
                                        'qty': self.qty,
                                        'cv': self.qty*self.exit_price if self.exit_price else 0,
                                        'trade': self.exit_trade,
                                        'system action': 'SHORT_EXIT' 
                                    }
                        )

                        signals_list.append(
                                    {
                                        'timestamp': self.row.timestamp,
                                        'symbol': f'{self.underlying}-FUT',
                                        'price': self.entry_price,
                                        'qty': self.qty,
                                        'cv': self.qty*self.entry_price if self.entry_price else 0,
                                        'trade': self.entry_trade,
                                        'system action': 'LONG_ENTRY' 
                                    }
                        )

                    elif self.in_position != 1:
                        self.tick = self.ck.get_fut_tick(self.underlying, self.expiry_code, self.row.timestamp.date(), self.row.timestamp.time())
                        self.entry_price = self.tick.get('c') if self.tick else np.nan
                        self.entry_trade = 'BUY'
                        self.in_position = 1
                        print('ADDING LONG ENTRY')
                        signals_list.append(
                                    {
                                        'timestamp': self.row.timestamp,
                                        'symbol': f'{self.underlying}-FUT',
                                        'price': self.entry_price,
                                        'qty': self.qty,
                                        'cv': self.qty*self.entry_price if self.entry_price else 0,
                                        'trade': self.entry_trade,
                                        'system action': 'LONG_ENTRY' 
                                    }
                        )

                if self.row.c < self.row.ma:
                    if self.in_position == 1:
                        self.tick = self.ck.get_fut_tick(self.underlying, self.expiry_code, self.row.timestamp.date(), self.row.timestamp.time())
                        self.exit_price = self.tick.get('c') if self.tick else np.nan
                        self.exit_trade = 'SELL'
                        self.entry_price = self.exit_price
                        self.entry_trade = 'SHORT'
                        self.in_position = -1
                        signals_list.append(
                                    {
                                        'timestamp': self.row.timestamp,
                                        'symbol': f'{self.underlying}-FUT',
                                        'price': self.exit_price,
                                        'qty': self.qty,
                                        'cv': self.qty*self.exit_price if self.exit_price else np.nan,
                                        'trade': self.exit_trade,
                                        'system action': 'LONG_EXIT' 
                                    }
                        )
                        signals_list.append(
                                    {
                                        'timestamp': self.row.timestamp,
                                        'symbol': f'{self.underlying}-FUT',
                                        'price': self.entry_price,
                                        'qty': self.qty,
                                        'cv': self.qty*self.entry_price if self.entry_price else np.nan,
                                        'trade': self.entry_trade,
                                        'system action': 'SHORT_ENTRY' 
                                    }
                        )

                    elif self.in_position != -1:
                        self.tick = self.ck.get_fut_tick(self.underlying, self.expiry_code, self.row.timestamp.date(), self.row.timestamp.time())
                        self.entry_price = self.tick.get('c') if self.tick else np.nan
                        self.entry_trade = 'SHORT'
                        self.in_position = -1
                        print('ADDING SHORT ENTRY')
                        signals_list.append(
                                    {
                                        'timestamp': self.row.timestamp,
                                        'symbol': f'{self.underlying}-FUT',
                                        'price': self.entry_price,
                                        'qty': self.qty,
                                        'cv': self.qty*self.entry_price if self.entry_price else 0,
                                        'trade': self.entry_trade,
                                        'system action': 'SHORT_ENTRY' 
                                    }
                        )

        return signals_list

    def create_backtest(self, uid: str):
        start = time.time()
        self.set_signal_parameters(uid)
        signals = self.gen_signals()
        df = pd.DataFrame(signals)
        tradesheet = self.metrics.calculate_pl_in_positional_tradesheet(df)
        tradesheet.to_parquet(f'C:/Users/Prahlad/108_research/tradesheets/pricemaclosefilter/{uid}.parquet')
        end = time.time()
        print(f'Elapsed Time In COMPLETING raw Tradesheet Generation: {end-start}')
        print("+++++++++++++++++++++++++++++++++++++++ GENERATED UID +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

