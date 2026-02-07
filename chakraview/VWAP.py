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
        self.supertrend_period = int(uid_split.pop(0))
        self.multiplier = int(uid_split.pop(0))
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
    
    def create_itertuples(self, db):
        return db.itertuples(index = False)

    def calculate_chandelier_exit(self, db):
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

        db['chandelier_long'] = db['highest_high'] - self.multiplier * db['atr']
        db['chandelier_short'] = db['lowest_low'] + self.multiplier * db['atr']

        return db
    
    def calculate_supertrend(self, db):
        
        df = db.copy()
        
        # Calculate True Range
        prev_close = df['c'].shift(1)
        tr = np.maximum.reduce([
            df['h'] - df['l'],
            (df['h'] - prev_close).abs(),
            (df['l'] - prev_close).abs()
        ])
        
        # Calculate ATR - keep as Series to use rolling
        atr = pd.Series(tr).rolling(self.supertrend_period).mean().to_numpy()
        
        # Calculate HL/2 (median price)
        hl2 = ((df['h'] + df['l']) / 2).to_numpy()
        
        # Calculate basic bands
        basic_upper = hl2 + self.multiplier * atr
        basic_lower = hl2 - self.multiplier * atr
        
        close = df['c'].to_numpy()
        n = len(df)
        
        # Initialize arrays
        final_upper = np.full(n, np.nan)
        final_lower = np.full(n, np.nan)
        supertrend = np.full(n, np.nan)
        trend = np.full(n, 0)
        
        # Find first valid index
        start = np.where(~np.isnan(atr))[0][0]
        
        # Initialize first values
        final_upper[start] = basic_upper[start]
        final_lower[start] = basic_lower[start]
        
        # Determine initial trend
        if close[start] <= final_upper[start]:
            trend[start] = -1
            supertrend[start] = final_upper[start]
        else:
            trend[start] = 1
            supertrend[start] = final_lower[start]
        
        # Calculate SuperTrend
        for i in range(start + 1, n):
            # Update final bands
            if basic_upper[i] < final_upper[i-1] or close[i-1] > final_upper[i-1]:
                final_upper[i] = basic_upper[i]
            else:
                final_upper[i] = final_upper[i-1]
            
            if basic_lower[i] > final_lower[i-1] or close[i-1] < final_lower[i-1]:
                final_lower[i] = basic_lower[i]
            else:
                final_lower[i] = final_lower[i-1]
            
            # Determine trend
            if trend[i-1] == 1:
                if close[i] <= final_lower[i]:
                    trend[i] = -1
                else:
                    trend[i] = 1
            else:  # trend[i-1] == -1
                if close[i] >= final_upper[i]:
                    trend[i] = 1
                else:
                    trend[i] = -1
            
            # Set SuperTrend value
            if trend[i] == 1:
                supertrend[i] = final_lower[i]
            else:
                supertrend[i] = final_upper[i]
        
        df['final_upper'] = final_upper
        df['final_lower'] = final_lower
        df['supertrend'] = supertrend
        df['trend'] = trend
        
        return df

    def gen_signals(self):
        pass

    
