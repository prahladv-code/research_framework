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

    def generate_signals(self):
        pass
