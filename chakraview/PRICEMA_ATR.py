from chakraview.chakraview import ChakraView
import datetime
import pandas as pd
from chakraview.config import sessions
import multiprocessing as mp
from analysis.calculate_metrics import CalculateMetrics
import numpy as np
import time

class PRICEMA:
    def __init__(self):
        self.ck = ChakraView()
        self.db = self.ck.get_df('nifty_fut')
        self.start_time = sessions.get('nifty_fut').get('start')
        self.end_time = sessions.get('nifty_fut').get('end')
        self.previous_date = None
        self.metrics = CalculateMetrics()
    
    def reset_all_variables(self):
        self.entry_price = np.nan
        self.exit_price = np.nan
        self.short_condition = False
        self.long_condition = False
        self.entry_trade = ''
        self.entry_time = None
        self.entry_price = None
        self.position_count = 0
        self.last_signal = None
        self.wait_for_cross = False
    
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
        self.ma_period = int(uid_split.pop(0))
        # self.atr_stochastic_period = int(uid_split.pop(0))
        self.timeframe = uid_split.pop(0)
        self.reentry = uid_split.pop(0) == 'True'
        self.trailing_stop_period = self.ma_period / 2
    
    def create_itertupes(self, db):
        return db.itertuples(index=False)
    
    def generate_uid_from_parameters(self, ma_period, timeframe, reentry):
        return f'PRICEMAATR_niftyfut_{ma_period}_{timeframe}_{reentry}'
    
    def calculate_atr_indicator(self, db, ma_period):
        db['prev_close'] = db['close'].shift(1)

        db['tr'] = np.maximum.reduce([
            db['high'] - db['low'],
            (db['high'] - db['prev_close']).abs(),
            (db['low'] - db['prev_close']).abs()
        ])
        db['normalised_tr'] = (db['tr']/db['close'])*100
        db['atr_ma'] = db['normalised_tr'].rolling(ma_period).mean()
        return db
    
    def calculate_stochastic(self, db, period=14, smooth_k=3):
        # Calculate rolling highest high and lowest low
        db['highest_high'] = db['high'].rolling(period).max()
        db['lowest_low'] = db['low'].rolling(period).min()

        # Raw %K Stochastic
        db['stochastic'] = ((db['close'] - db['lowest_low']) /
                    (db['highest_high'] - db['lowest_low'])) * 100

        # Smooth %K (3-period SMA default)
        db['stochastic_ma'] = db['stochastic'].rolling(smooth_k).mean()

        return db

    def calculate_chandelier_exit(self, db, multiplier=3):
        """
        Calculates chandelier long and short trailing stop based on ATR
        using the self.trailing_stop_period lookback window
        """
        period = int(self.trailing_stop_period)

        # compute ATR the correct way
        db['prev_close'] = db['close'].shift(1)
        db['tr'] = np.maximum.reduce([
            db['high'] - db['low'],
            (db['high'] - db['prev_close']).abs(),
            (db['low'] - db['prev_close']).abs()
        ])
        db['atr'] = db['tr'].rolling(period).mean()

        # Chandelier Exit bands
        db['highest_high'] = db['high'].rolling(period).max()
        db['lowest_low'] = db['low'].rolling(period).min()

        db['chandelier_long'] = db['highest_high'] - multiplier * db['atr']
        db['chandelier_short'] = db['lowest_low'] + multiplier * db['atr']

        return db


    def gen_signals(self):
        resample_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
        }
        signals_list = []
        df = self.db.copy()
        df['timestamp'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))
        df = df.set_index('timestamp')
        iterable_df = (
            df.resample(f'{self.timeframe}min')
            .agg(resample_dict)
            .dropna(how='all')
            .reset_index()
        )
        iterable_df['ma'] = iterable_df['close'].rolling(self.ma_period).mean()
        # stochastic_plugin = self.calculate_stochastic(iterable_df)
        chandelier_plugin = self.calculate_chandelier_exit(iterable_df)
        iterable_df = chandelier_plugin.copy()
        iterable = self.create_itertupes(iterable_df)
        self.in_position = 0
        for self.row in iterable:
            # if self.last_signal:
            #     if self.last_signal == 'LONG' and self.row.close < self.row.ma:
            #         self.wait_for_cross = False

            #     if self.last_signal == 'SHORT' and self.row.close > self.row.ma:
            #         self.wait_for_cross = False

            new_day = self.check_new_day(self.row.timestamp.date())

            #ENTRY
            if self.row.close > self.row.ma and self.row.stochastic > self.row.stochastic_ma:
                if self.in_position == -1:
                    self.exit_price = self.row.close
                    self.exit_trade = 'COVER'
                    self.entry_price = self.row.close
                    self.entry_trade = 'BUY'
                    self.in_position = 1
                    signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.exit_price,
                                    'qty': 75,
                                    'cv': 75*self.exit_price if self.exit_price else 0,
                                    'trade': self.exit_trade,
                                    'system action': 'SHORT_EXIT' 
                                }
                    )
                    signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.exit_price,
                                    'qty': 75,
                                    'cv': 75*self.exit_price if self.exit_price else 0,
                                    'trade': 'MTM_COVER',
                                    'system action': 'PSEUDO_SHORT_EXIT' 
                                }
                    )
                    signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.entry_price,
                                    'qty': 75,
                                    'cv': 75*self.entry_price if self.entry_price else 0,
                                    'trade': self.entry_trade,
                                    'system action': 'LONG_ENTRY' 
                                }
                    )
                    signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.entry_price,
                                    'qty': 75,
                                    'cv': 75*self.entry_price if self.entry_price else 0,
                                    'trade': 'MTM_BUY',
                                    'system action': 'PSEUDO_LONG_ENTRY' 
                                }
                    )
                elif self.in_position != 1:
                    self.entry_price = self.row.close
                    self.entry_trade = 'BUY'
                    self.in_position = 1
                    print('ADDING LONG ENTRY')
                    signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.entry_price,
                                    'qty': 75,
                                    'cv': 75*self.entry_price if self.entry_price else 0,
                                    'trade': self.entry_trade,
                                    'system action': 'LONG_ENTRY' 
                                }
                    )
                    signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.entry_price,
                                    'qty': 75,
                                    'cv': 75*self.entry_price if self.entry_price else 0,
                                    'trade': 'MTM_BUY',
                                    'system action': 'PSEUDO_LONG_ENTRY' 
                                }
                    )

            if self.row.close < self.row.ma and self.row.stochastic > self.row.stochastic_ma:
                if self.in_position == 1:
                    self.exit_price = self.row.close
                    self.exit_trade = 'SELL'
                    self.entry_price = self.row.close
                    self.entry_trade = 'SHORT'
                    self.in_position = -1
                    signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.exit_price,
                                    'qty': 75,
                                    'cv': 75*self.exit_price if self.exit_price else 0,
                                    'trade': self.exit_trade,
                                    'system action': 'LONG_EXIT' 
                                }
                    )
                    signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.exit_price,
                                    'qty': 75,
                                    'cv': 75*self.exit_price if self.exit_price else 0,
                                    'trade': f'MTM_{self.exit_trade}',
                                    'system action': 'PSEUDO_LONG_EXIT' 
                                }
                    )
                    signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.entry_price,
                                    'qty': 75,
                                    'cv': 75*self.entry_price if self.entry_price else 0,
                                    'trade': self.entry_trade,
                                    'system action': 'SHORT_ENTRY' 
                                }
                    )
                    signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.entry_price,
                                    'qty': 75,
                                    'cv': 75*self.entry_price if self.entry_price else 0,
                                    'trade': f'MTM_{self.entry_trade}',
                                    'system action': 'PSEUDO_SHORT_ENTRY' 
                                }
                    )
                elif self.in_position != -1:
                    self.entry_price = self.row.close
                    self.entry_trade = 'SHORT'
                    self.in_position = -1
                    print('ADDING SHORT ENTRY')
                    signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.entry_price,
                                    'qty': 75,
                                    'cv': 75*self.entry_price if self.entry_price else 0,
                                    'trade': self.entry_trade,
                                    'system action': 'SHORT_ENTRY' 
                                }
                    )
                    signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.entry_price,
                                    'qty': 75,
                                    'cv': 75*self.entry_price if self.entry_price else 0,
                                    'trade': f'MTM_{self.entry_trade}',
                                    'system action': 'PSEUDO_SHORT_ENTRY' 
                                }
                    )

            # ADDING SEPARATE EXITS
            if self.row.close > self.row.ma and self.in_position == -1:
                print("ADDING COVER TRADE")
                self.exit_price = self.row.close
                self.exit_trade = 'COVER'
                signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.exit_price,
                                    'qty': 75,
                                    'cv': 75*self.exit_price if self.exit_price else 0,
                                    'trade': self.exit_trade,
                                    'system action': 'SHORT_EXIT' 
                                }
                    )
                self.in_position = 0
            
            if self.row.close < self.row.ma and self.in_position == 1:
                self.exit_price = self.row.close
                self.exit_trade = 'SELL'
                signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.exit_price,
                                    'qty': 75,
                                    'cv': 75*self.exit_price if self.exit_price else 0,
                                    'trade': self.exit_trade,
                                    'system action': 'LONG_EXIT' 
                                }
                    )
                self.in_position = 0



            # PSEUDO ENTRY
            if new_day:
                print(f'NEW_DAY: {self.row.timestamp.date()}')
                self.pseudo_exit_flag = 0
                self.pseudo_entry_flag = 0
                if self.in_position == 1:
                    print('ADDING LONG PSEUDO ENTRY')
                    self.entry_price = self.row.close
                    self.entry_trade = 'MTM_BUY'
                    if self.pseudo_entry_flag == 0:
                        self.pseudo_entry_flag = 1
                        signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.entry_price,
                                    'qty': 75,
                                    'cv': 75*self.entry_price if self.entry_price else 0,
                                    'trade': self.entry_trade,
                                    'system action': 'PSEUDO_LONG_ENTRY' 
                                }
                            )
                elif self.in_position == -1:
                    print('ADDING SHORT PSEUDO ENTRY')
                    self.entry_price = self.row.close
                    self.entry_trade = 'MTM_SHORT'
                    if self.pseudo_entry_flag == 0:
                        self.pseudo_entry_flag = 1
                        signals_list.append(
                                {
                                    'timestamp': self.row.timestamp,
                                    'symbol': 'NIFTY-FUT',
                                    'price': self.entry_price,
                                    'qty': 75,
                                    'cv': 75*self.entry_price if self.entry_price else 0,
                                    'trade': self.entry_trade,
                                    'system action': 'PSEUDO_SHORT_ENTRY' 
                                }
                            )
            
            #PSEUDO EXIT
            if self.pseudo_exit_flag == 0:
                if self.row.timestamp.time() >= self.end_time:
                    print('ADDING PSEUDO EXIT')
                    if self.in_position == 1:
                        self.exit_price = self.row.close
                        self.exit_trade ='MTM_SELL'
                        if self.pseudo_exit_flag == 0:
                            self.pseudo_exit_flag = 1
                            signals_list.append(
                                        {
                                            'timestamp': self.row.timestamp,
                                            'symbol': 'NIFTY-FUT',
                                            'price': self.exit_price,
                                            'qty': 75,
                                            'cv': 75*self.exit_price if self.exit_price else 0,
                                            'trade': self.exit_trade,
                                            'system action': 'PSEUDO_LONG_EXIT' 
                                        }
                                    )
                    
                    elif self.in_position == -1:
                        self.exit_price = self.row.close
                        self.exit_trade = 'MTM_COVER'
                        if self.pseudo_exit_flag == 0:
                            self.pseudo_exit_flag = 1
                            signals_list.append(
                                        {
                                            'timestamp': self.row.timestamp,
                                            'symbol': 'NIFTY-FUT',
                                            'price': self.exit_price,
                                            'qty': 75,
                                            'cv': 75*self.exit_price if self.exit_price else 0,
                                            'trade': self.exit_trade,
                                            'system action': 'PSEUDO_SHORT_EXIT' 
                                        }
                                    )
        return signals_list

    def create_backtest(self, uid: str):
        start = time.time()
        self.set_signal_parameters(uid)
        signals = self.gen_signals()
        df = pd.DataFrame(signals)
        tradesheet = self.metrics.calculate_pl_in_positional_tradesheet(df)
        tradesheet.to_parquet(f'C:/Users/admin/VSCode/research/research_framework/tradesheets/pricema_atr/{uid}.parquet')
        # tradesheet.to_csv(r'C:\Users\admin\VSCode\PRICEMA_niftyfut_33_25_False.csv')
        end = time.time()
        print(f'Elapsed Time In COMPLETING raw Tradesheet Generation: {end-start}')
        print("+++++++++++++++++++++++++++++++++++++++ GENERATED UID +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

