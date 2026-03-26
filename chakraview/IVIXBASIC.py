from chakraview.chakraview import ChakraView
import pandas as pd
import numpy as np
import duckdb
import time
from chakraview.config import sessions, lot_sizes, strike_diff
import datetime
import time
from chakraview.config import r
import re
from chakraview.logger import logger
from analysis.calculate_metrics import CalculateMetrics

class IVIX(ChakraView):
    def __init__(self):
        super().__init__()
        self.live_positions = []
        self.position_count = 0
        self.new_expiry = None
        self.expiry = 0
        self.protection_call = None
    
    def create_iterable(self, df):
        return df.itertuples()
    
    #"IVIX_NIFTY_60_7_20_10_True_3_0"
    def set_params_from_uid(self, uid):
        self.uid_split = uid.split('_')
        self.strat = self.uid_split.pop(0)
        self.underlying = self.uid_split.pop(0)
        self.timeframe = self.uid_split.pop(0)
        self.indicator_period = int(self.uid_split.pop(0))
        self.short_delta = float(self.uid_split.pop(0))
        self.long_delta = float(self.uid_split.pop(0))
        self.max_entries = self.uid_split.pop(0) == "True"
        self.max_entry_num = int(self.uid_split.pop(0)) if self.max_entries else None
        self.expiry_code = int(self.uid_split.pop(0))
        self.start_time = sessions.get(self.underlying).get('start')
        self.end_time = sessions.get(self.underlying).get('end')
        self.int_timeframe = int(self.timeframe)
        self.qty = lot_sizes.get(self.underlying)

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
                'straddle_price': 'last'
            })

            resampled_list.append(resampled)

        resampled_df = pd.concat(resampled_list)

        resampled_df = resampled_df.reset_index(drop=False)
        resampled_df['date'] = resampled_df['timestamp'].dt.date
        resampled_df['time'] = resampled_df['timestamp'].dt.time

        return resampled_df.dropna()
    

    def generate_straddle_indicator(self, resampled_df):
        resampled_df['new_day'] = resampled_df['date'] != resampled_df['date'].shift()
        resampled_df['first_straddle_price'] = np.where(resampled_df['new_day'], resampled_df['straddle_price'], np.nan)
        resampled_df['roc'] = np.where(resampled_df['first_straddle_price'].notna(), resampled_df['straddle_price']/resampled_df['first_straddle_price'], resampled_df['straddle_price']/resampled_df['straddle_price'].shift())
        resampled_df['roc'] = resampled_df['roc'].ffill()
        resampled_df['rolling_high'] = resampled_df['roc'].rolling(self.indicator_period).max()
        resampled_df['rolling_low'] = resampled_df['roc'].rolling(self.indicator_period).min()
        resampled_df['stochastic'] = 100 - (((resampled_df['roc'] - resampled_df['rolling_low'])/(resampled_df['rolling_high'] - resampled_df['rolling_low'])) * 100)
        resampled_df['inverse_stochastic_indicator'] = resampled_df['stochastic'].rolling(3).mean()
        return resampled_df
    
    def generate_signal_iterable(self):
        """
        Generates Signal iterable by calculating straddle indicator"

        """
        straddle_df = self.get_spot_df(f'{self.underlying}_straddles')
        resampled_df = self.resample_df(straddle_df)
        indicator_df = self.generate_straddle_indicator(resampled_df)
        signal_iterable = self.create_iterable(indicator_df)
        return signal_iterable
    

    def get_all_condor_tickers(self, date, time):
        self.short_call = self.find_ticker_by_delta(self.underlying, date, time, self.short_delta, 'CE', self.expiry_code)
        self.short_put = self.find_ticker_by_delta(self.underlying, date, time, self.short_delta, 'PE', self.expiry_code)
        self.protection_put = self.find_ticker_by_delta(self.underlying, date, time, self.long_delta, 'CE', self.expiry_code)
        
        if self.short_call and self.short_put and self.protection_put:
            short_put_strike = self.short_put['strike']
            protection_put_strike = self.protection_put['strike']
            short_call_strike = self.short_call['strike']
            strike_diff = short_put_strike - protection_put_strike
            protection_call_strike = short_call_strike + strike_diff
            self.protection_call = self.find_ticker_by_strike(self.underlying, date, time, protection_call_strike, 'CE', self.expiry_code)

        if self.short_call and self.short_put and self.protection_put and self.protection_call:
            return True
        else:
            return False

    def check_new_expiry(self, expiry):
        if expiry == self.new_expiry:
            return False
        if expiry != self.new_expiry:
            self.new_expiry = expiry
            return True


    def calculate_payoff(self):
        short_strike_call = self.short_call['strike']
        short_strike_put = self.short_put['strike']
        short_entry_call = self.short_call['c']
        short_entry_put = self.short_put['c']
        protection_entry_call = self.protection_call['c']
        protection_entry_put = self.protection_put['c']
        put_net_credit = short_entry_put - protection_entry_put
        call_net_credit = short_entry_call - protection_entry_call
        put_breakeven = short_strike_put - put_net_credit
        call_breakeven = short_strike_call + call_net_credit
        breakevens = [call_breakeven, put_breakeven]
        return breakevens
    
    def generate_resampled_timestamp(self, date: datetime.date, time: datetime.time):
        timestamp = datetime.datetime.combine(date, time)
        timestamp_offset = self.int_timeframe - 1
        adjusted_timestamp = timestamp + datetime.timedelta(minutes=timestamp_offset)
        return adjusted_timestamp.time()

    def generate_signal(self):
        self.signal_list = []
        iterable = self.generate_signal_iterable()
        end_dt = datetime.datetime.combine(datetime.date(2025, 12, 31), self.end_time)
        end_dt -= datetime.timedelta(minutes=(self.int_timeframe - 1))
        self.end_time = end_dt.time()
        for row in iterable:
            self.time = row.time
            self.date = row.date
            # ENTRY
            if row.inverse_stochastic_indicator < 20:
                adjusted_timestamp = self.generate_resampled_timestamp(row.date, row.time)
                data_check = self.get_all_condor_tickers(self.date, adjusted_timestamp)
                expiry = self.short_call.get('expiry')
                expiry_check = self.check_new_expiry(expiry)
                if expiry_check:
                    self.position_count = 0

                if self.position_count <= self.max_entry_num:
                    if data_check:
                        self.breakevens = self.calculate_payoff()
                        if len(self.breakevens) < 2:
                            print(f'NOT ENOUGH BREAKEVENS')
                            continue  # prevents break of logic
                        self.position_count += 1
                        self.live_positions.append(
                            {
                                'system_tag': f'IVIX{self.position_count}',
                                'short_call': self.short_call.get('symbol'),
                                'short_put': self.short_put.get('symbol'),
                                'protection_call': self.protection_call.get('symbol'),
                                'protection_put': self.protection_put.get('symbol'),
                                'lower_breakeven': self.breakevens[0],
                                'upper_breakeven': self.breakevens[1],
                                'expiry': expiry,
                                'active_legs': {'short_call', 'short_put', 'protection_call', 'protection_put'}
                            }
                        )
                        self.signal_list.append(self.place_trade(f'{row.date} {row.time}', 
                                                                self.short_call.get('symbol'), 
                                                                self.short_call.get('c'),
                                                                self.qty,
                                                                self.qty*self.short_call.get('c'),
                                                                'SHORT',
                                                                'SHORT_ENTRY'
                                                                ))
                        
                        self.signal_list.append(self.place_trade(f'{row.date} {row.time}', 
                                                                self.short_put.get('symbol'), 
                                                                self.short_put.get('c'),
                                                                self.qty,
                                                                self.qty*self.short_put.get('c'),
                                                                'SHORT',
                                                                'SHORT_ENTRY'
                                                                ))
                        
                        self.signal_list.append(self.place_trade(f'{row.date} {row.time}', 
                                                                self.protection_call.get('symbol'), 
                                                                self.protection_call.get('c'),
                                                                self.qty,
                                                                self.qty*self.protection_call.get('c'),
                                                                'BUY',
                                                                'LONG_ENTRY'
                                                                ))
                        
                        self.signal_list.append(self.place_trade(f'{row.date} {row.time}', 
                                                                self.protection_put.get('symbol'), 
                                                                self.protection_put.get('c'),
                                                                self.qty,
                                                                self.qty*self.protection_put.get('c'),
                                                                'BUY',
                                                                'LONG_ENTRY'
                                                                ))

           #EXITS
            exit_tags = []
            if self.live_positions:
                for pos_dict in self.live_positions:
                    print(f'EXPIRY: {pos_dict["expiry"]}')
                    print(f'TODAY {self.date} {self.time}')
                    print(f'END TIME {self.end_time}')
                    if row.c >= pos_dict['upper_breakeven'] and 'short_call' in pos_dict['active_legs']:
                        adjusted_timestamp = self.generate_resampled_timestamp(self.date, self.time)
                        exit_tick_short_call = self.get_tick(pos_dict['short_call'], self.date, adjusted_timestamp)
                        exit_tick_protection_call = self.get_tick(pos_dict['protection_call'], self.date, adjusted_timestamp)
                        if exit_tick_short_call and exit_tick_protection_call:
                            self.signal_list.append(self.place_trade(
                                f'{row.date} {row.time}', pos_dict['short_call'], exit_tick_short_call.get('c'), self.qty, self.qty*exit_tick_short_call.get('c'), 'COVER', 'SHORT_EXIT'
                            ))
                            self.signal_list.append(self.place_trade(
                                f'{row.date} {row.time}', pos_dict['protection_call'], exit_tick_protection_call.get('c'), self.qty, self.qty * exit_tick_protection_call.get('c'), 'SELL', 'LONG_EXIT'
                            ))
                            pos_dict['active_legs'].discard('short_call')
                            pos_dict['active_legs'].discard('protection_call')
                            if not pos_dict['active_legs']:
                                exit_tags.append(pos_dict['system_tag'])
                    
                    elif row.c <= pos_dict['lower_breakeven'] and 'short_put' in pos_dict['active_legs']:
                        adjusted_timestamp = self.generate_resampled_timestamp(self.date, self.time)
                        exit_tick_short_put = self.get_tick(pos_dict['short_put'], self.date, adjusted_timestamp)
                        exit_tick_protection_put = self.get_tick(pos_dict['protection_put'], self.date, adjusted_timestamp)
                        if exit_tick_protection_put and exit_tick_short_put:
                            self.signal_list.append(self.place_trade(
                                f'{row.date} {row.time}', pos_dict['short_put'], exit_tick_short_put.get('c'), self.qty, self.qty*exit_tick_short_put.get('c'), 'COVER', 'SHORT_EXIT'
                            ))
                            self.signal_list.append(self.place_trade(
                                f'{row.date} {row.time}', pos_dict['protection_put'], exit_tick_protection_put.get('c'), self.qty, self.qty * exit_tick_protection_put.get('c'), 'SELL', 'LONG_EXIT'
                            ))
                            pos_dict['active_legs'].discard('short_put')
                            pos_dict['active_legs'].discard('protection_put')
                            if not pos_dict['active_legs']:
                                exit_tags.append(pos_dict['system_tag'])
                    
                    elif self.date == pos_dict['expiry'] and self.time == self.end_time:
                        print("###################################################CONDOR EXPIRED#################################################")
                        adjusted_timestamp = self.generate_resampled_timestamp(row.date, row.time)
                        leg_map = {
                            'short_call':      (pos_dict['short_call'],      'COVER', 'SHORT_EXIT'),
                            'short_put':       (pos_dict['short_put'],       'COVER', 'SHORT_EXIT'),
                            'protection_call': (pos_dict['protection_call'], 'SELL',  'LONG_EXIT'),
                            'protection_put':  (pos_dict['protection_put'],  'SELL',  'LONG_EXIT'),
                        }
                        for leg_name in list(pos_dict['active_legs']):
                            symbol, direction, tag = leg_map[leg_name]
                            tick = self.get_tick(symbol, self.date, adjusted_timestamp)
                            if tick:
                                self.signal_list.append(self.place_trade(
                                    f'{row.date} {row.time}', symbol, tick.get('c'),
                                    self.qty, self.qty * tick.get('c'), direction, tag
                                ))
                        exit_tags.append(pos_dict['system_tag'])

                if exit_tags:
                    self.live_positions = [
                        pos for pos in self.live_positions
                        if pos['system_tag'] not in exit_tags
                    ]
                    self.position_count = len(self.live_positions)

        return self.signal_list
    
    def run_backtest(self, uid: str):
        calc = CalculateMetrics()
        self.set_params_from_uid(uid)
        trades = self.generate_signal()
        tradesheet = pd.DataFrame(trades)
        tradesheet = calc.calculate_pl_in_opt_tradesheet(tradesheet)
        tradesheet.to_parquet(f"C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/ivix/{uid}.parquet")
        print("+++++++++++++++++++++++++++++++++++++++++++BACKTEST COMPLETE++++++++++++++++++++++++++++++++++++++++++")

