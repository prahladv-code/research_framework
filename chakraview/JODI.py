import pandas as pd
# from analysis import calculate_metrics
from chakraview import ChakraView
import numpy as np
import datetime
import time
import logging
from config import sessions



class JODI(ChakraView):
    def __init__(self):
        super().__init__()
        self.start = sessions.get('nifty').get('start')
        self.end = sessions.get('nifty').get('end')
        self.previous_date = None
        self.signal_list = []

    
    def reset_all_variables(self):
        self.in_position = 0
        self.long_call_leg = {}
        self.long_put_leg = {}
        self.short_call_leg = {}
        self.short_put_leg = {}


    def check_newday(self, date):
        self.date = date
        if self.date != self.previous_date:
            self.previous_date = self.date
            return True
        elif self.date == self.previous_date:
            return False

    def set_signal_params(self, uid: str):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.ma_period = int(uid_split.pop(0))
    
    def create_iterable(self, db):
        df = self.get_df(db)
        return df.itertuples(index=False)
    
    def gen_signals(self, row):

        new_day = self.check_newday(row.date)

        if new_day:
            self.reset_all_variables()
        
        if row.close > row.ma:
            # ENTRY
            if self.in_position == -1:
                self.in_position = 1
                exit_call_symbol = self.short_call_leg.get('symbol')
                exit_put_symbol = self.short_put_leg.get('symbol')
                exit_tick_call = self.get_tick(exit_call_symbol, row.date, row.time)
                exit_tick_put = self.get_tick(exit_put_symbol, row.date, row.time)
                timestamp = str(row.date) + str(row.time)
                if exit_tick_call and exit_tick_put:
                    exit_price_call = exit_tick_call.get('c')
                    exit_price_put = exit_tick_put.get('c')
                    exit_call_trade = self.place_trade(timestamp, exit_call_symbol, exit_price_call, 75, 75 * exit_price_call, 'COVER', 'SHORT_EXIT')
                    exit_put_trade = self.place_trade(timestamp, exit_put_symbol, exit_price_put, 75, 75 * exit_price_put, 'SELL', 'SHORT_EXIT')
                    self.signal_list.append(exit_call_trade)
                    self.signal_list.append(exit_put_trade)

                self.long_call_leg = self.find_ticker_by_moneyness(row.date, row.time, row.close, 50, 'CE', 0)
                self.long_put_leg = self.find_ticker_by_moneyness(row.date, row.time, row.close, 50, 'PE', 0)
                if self.long_call_leg and self.long_put_leg:
                    entry_call_symbol = self.long_call_leg.get('symbol')
                    entry_put_symbol = self.long_put_leg.get('symbol')
                    entry_price_call = self.long_call_leg.get('c')
                    entry_price_put = self.long_put_leg.get('c')
                    entry_call_trade = self.place_trade(timestamp, entry_call_symbol, entry_price_call, 75, 75 * entry_price_call, 'BUY', 'LONG_ENTRY')
                    entry_put_trade = '' # DEV PAUSED





j = JODI()
iter = j.create_iterable('nifty')
for row in iter:
    print(row)
    
    