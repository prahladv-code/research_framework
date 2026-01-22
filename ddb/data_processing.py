import pandas as pd
import numpy as np
import datetime
import time
import gzip
import re

class DataProcessing:

    def __init__(self):
        """
        Takes In DataFrame And Processes Data Points like strike, greeks and expiries for further processing. Converts Raw Data Into Processed Parquet files.
        """

        self.KNOWN_INDICES = ["NIFTYNXT50"]
        self.combined_df = []
    
    def parse_option_symbol(self, symbol: str) -> dict:

        # 1️⃣ Check index symbols first (critical!)
        for idx in self.KNOWN_INDICES:
            if symbol.startswith(idx):
                return

        # 2️⃣ Fallback: stock options
        pattern = r"""
            ^(?P<underlying>[A-Z0-9&\-]+?)  # underlying
            (?P<expiry>\d{6})               # YYMMDD
            (?P<strike>\d+(?:\.\d+)?)       # strike (int or decimal)
            (?P<right>CE|PE)$               # option type
        """

        m = re.match(pattern, symbol, re.VERBOSE)
        if not m:
            raise ValueError(f"Invalid option symbol: {symbol}")
        
        processed_symbol = m.groupdict()
        underlying = processed_symbol.get('underlying')
        expiry = datetime.datetime.strptime(processed_symbol.get('expiry'), '%y%m%d')
        strike = float(processed_symbol.get('strike'))
        right = processed_symbol.get('right')
        return {'underlying': underlying, 'strike': strike, 'right': right, 'expiry': expiry}


    def process_options_df(self, df, symbol, meta):
        """Processes A Given DataFrame And Returns A Robust And Customizable """
        underlying = meta.get('underlying')
        strike = meta.get('strike')
        right = meta.get('right')
        expiry = meta.get('expiry')
        processed_df = df.copy()
        processed_df.columns = ['date', 'time', 'o', 'h', 'l', 'c', 'v', 'oi']
        processed_df['symbol'] = symbol
        processed_df['underlying'] = underlying
        processed_df['strike'] = strike
        processed_df['right'] = right
        processed_df['expiry'] = expiry
        processed_df['date'] = pd.to_datetime(processed_df['date'], format='%Y%m%d', errors='coerce').dt.date
        processed_df['time'] = pd.to_datetime(processed_df['time'], format = '%H:%M', errors='coerce').dt.time
        
        return processed_df

    
