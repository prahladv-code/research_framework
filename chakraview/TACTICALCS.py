from chakraview import ChakraView
import pandas as pd
import numpy as np
import logging
import datetime
import time
# from analysis import calculate_metrics
from config import sessions


class TACTICALCS(ChakraView):
    def __init__(self):
        super().__init__()
        self.start = sessions.get('nifty').get('start')
        self.end = sessions.get('nifty').get('end')
        self.in_position = 0
        self.signal_list = []

    
    def get_expiry(self, symbol):
        if len(symbol) >= 12 and symbol.startswith("NIFTY"):
            part = symbol[5:12]  # DDMMMYY
            try:
                return datetime.datetime.strptime(part, "%d%b%y")
            except: 
                return pd.NaT
        return pd.NaT
    
    def set_signal_params(self, uid: str):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.timeframe = int(uid_split.pop(0))
        self.delta = float(uid_split.pop(0))
        self.calculate_breakeven_condition = uid_split.pop(0) == "True"
    
    def get_short_contract(self, date, time, spot, right):
        self.short_contract = self.find_ticker_by_delta(date, time, self.delta, right, spot)
        return self.short_contract

    def calculate_breakeven(self):
        strike = self.short_contract.get('strike')
        premium = self.short_contract.get('c')
        right = self.short_contract.get('right')

        if right == 'CE':
            return strike + premium
        elif right == 'PE':
            return strike - premium


    def create_iterable(self, df):
        return df.itertuples(index=False)

    def generate_db(self, db):
        pass

    def generate_signals(self, row):
        # ENTRY
        if row.close > row.ma:
            if self.in_position == -1:
                pass
            elif self.in_position != 1:
                self.get_short_contract(row.date, row.time, row.close, 'CE')
                timestamp = str(row.date) + str(row.time)
                if self.short_contract:
                    self.in_position = 1
                    entry_symbol = self.short_contract.get('symbol')
                    entry_price = self.short_contract.get('c')
                    entry_signal = self.place_trade(timestamp, entry_symbol, entry_price, 75, 75 * entry_price, 'SHORT', 'SHORT_ENTRY')
                    self.signal_list.append(entry_signal)
        
        if row.close < row.ma:
            if self.in_position == 1:
                pass
            elif self.in_position != -1:
                self.get_short_contract(row.date, row.time, row.close, 'PE')
                timestamp = str(row.date) + str(row.time)
                if self.short_contract:
                    self.in_position = -1
                    entry_symbol = self.short_contract.get('symbol')
                    entry_price = self.short_contract.get('c')
                    entry_signal = self.place_trade(timestamp, entry_symbol, entry_price, 75, 75 * entry_price, 'SHORT', 'SHORT_ENTRY')
                    self.signal_list.append(entry_signal)
        
        #EXIT
        if self.calculate_breakeven_condition:
            if self.in_position == -1:
                breakeven = self.calculate_breakeven()
                if row.close <= breakeven:
                    exit_symbol = self.short_contract.get('symbol')
                    exit_tick = self.get_tick(exit_symbol, row.date, row.time)
                    if exit_tick:
                        pass

