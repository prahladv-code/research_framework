from chakraview.chakraview import ChakraView
import pandas as pd
import numpy as np
import datetime
import time
from chakraview.config import sessions


class VWAP(ChakraView):
    def __init__(self):
        super().__init__()
    
    def set_params_from_uid(self, uid):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        session_name = self.underlying.lower()
        self.expiry_code = uid_split.pop(0)
        self.timeframe = uid_split.pop(0)
        self.start = sessions.get(session_name).get('start')
        self.end = sessions.get(session_name).get('end')
    
    def calculate_vwap(self, df):
        """Calculate VWAP For Iterable DataFrame"""
        df['Fair Price'] = (df['h'] + df['l'] + df['c'])/3
        df['Total Volume'] = df['v'].cumsum()
        df['Weighted Price'] = df['Fair Price'] * df['v']
        df['VWAP'] = df['weighted Price']/df['Total Volume']
        return df
    
    def get_relevant_options_dataframes(self, date: datetime.date, time: datetime.time, right: str, underlying_price: float):
        strike_details = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, date, time, underlying_price, 50, right, 0)
        symbol = strike_details.get('symbol')
        symbol_df = self.get_all_ticks_by_symbol(symbol)
        return symbol_df
    
    def yes(self):
        pass
    
    



