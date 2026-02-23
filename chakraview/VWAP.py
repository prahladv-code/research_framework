from chakraview.chakraview import ChakraView
import pandas as pd
import numpy as np
import datetime
import time
from chakraview.config import sessions
from analysis import calculate_metrics

class VWAP(ChakraView):
    def __init__(self):
        super().__init__()
        self.entry_condition_time = datetime.time(9, 16, 0)
        self.exit_condition_time = datetime.time(15, 27, 0)
        self.signal_list = []
        self.calc = calculate_metrics.CalculateMetrics()
        self.reset_all_variables()
    
    def set_params_from_uid(self, uid):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        session_name = self.underlying.lower()
        self.expiry_code = int(uid_split.pop(0))
        self.timeframe = uid_split.pop(0)
        self.supertrend_period = int(uid_split.pop(0))
        self.multiplier = int(uid_split.pop(0))
        self.start = sessions.get(session_name).get('start')
        self.end = sessions.get(session_name).get('end')
    
    def calculate_vwap(self, df):
        """Calculate VWAP For Iterable DataFrame"""
        df['fair_price'] = (df['h'] + df['l'] + df['c'])/3
        df['total_volume'] = df['v'].cumsum()
        df['weighted_price'] = df['fair_price'] * df['v']
        df['total_weighted_price'] = df['weighted_price'].cumsum()
        df['vwap'] = df['total_weighted_price']/df['total_volume']
        return df
    
    def reset_all_variables(self):
        self.entry_symbol_call = None
        self.entry_symbol_put = None
        self.in_position_call = 0
        self.in_position_put = 0

    def get_relevant_options_dataframes(self, date: datetime.date, time: datetime.time, right: str, underlying_price: float):
        strike_details = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, date, time, underlying_price, 50, right, 0)
        if strike_details:
            symbol = strike_details.get('symbol')
            symbol_df = self.get_all_ticks_by_symbol(symbol)
            symbol_df['date'] = pd.to_datetime(symbol_df['date'], format="%Y-%m-%d").dt.date
            symbol_df['time'] = pd.to_datetime(symbol_df['time'], format="%H:%M:%S").dt.time
            day_df = symbol_df[symbol_df['date'] == date]
            return day_df
        else:
            return pd.DataFrame()
    
    def create_itertuples(self, db):
        return db.itertuples(index=False)

    def calculate_chandelier_exit(self, db):
        """
        Calculates chandelier long and short trailing stop based on ATR
        using the self.trailing_stop_period lookback window
        """
        period = int(self.trailing_stop_period)

        # compute ATR the correct way
        db['prev_close'] = db['c'].shift(1)
        db['tr'] = np.maximum.reduce([
            db['h'] - db['l'],
            (db['h'] - db['prev_close']).abs(),
            (db['l'] - db['prev_close']).abs()
        ])
        db['atr'] = db['tr'].rolling(period).mean()

        # Chandelier Exit bands
        db['highest_high'] = db['h'].rolling(period).max()
        db['lowest_low'] = db['l'].rolling(period).min()

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

    def gen_signals_call(self, row):

        if row.time == self.start:
            print(f'Found Valid Time For DateTime: {row.date} {row.time}')
            call_df = self.get_relevant_options_dataframes(row.date, row.time, 'CE', row.c)
            if not call_df.empty and call_df is not None:
                vwap_df = self.calculate_vwap(call_df)
                call_vwap_itertuples = self.create_itertuples(vwap_df)
                print("Printing Itertuples Now")
                for vwap_row in call_vwap_itertuples:
                    if vwap_row.time >= self.entry_condition_time:
                        if vwap_row.c > vwap_row.vwap and self.in_position_call != 1:
                            current_timestamp = str(vwap_row.date) + '' + str(vwap_row.time)
                            if self.in_position_call == -1:
                                print(f'VWAP CALL SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                                if self.entry_symbol_call:
                                    exit_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                    exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                    exit_signal = self.place_trade(current_timestamp, self.entry_symbol_call, exit_tick, 65, 65*exit_tick, 'COVER', 'EXIT_SHORT')
                                    self.signal_list.append(exit_signal)
                                    self.in_position_call = 0
                                else:
                                    print(f'Call Symbol Is Empty. Skipping.')
                                    self.in_position_call = 0
                            
                                print(f'VWAP CALL LONG ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                                self.entry_symbol_call = vwap_row.symbol
                                if self.entry_symbol_call:
                                    entry_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                    entry_tick = entry_tick.get('c') if entry_tick else np.nan
                                    entry_signal = self.place_trade(current_timestamp, self.entry_symbol_call, entry_tick, 65, 65*entry_tick, "BUY", "ENTRY_LONG")
                                    self.in_position_call = 1
                                    self.signal_list.append(entry_signal)
                                else:
                                    print(f'Call Symbol Is Empty. Skipping.')
                                    self.in_position_call = 0

                            elif self.in_position_call == 0:
                                print(f'VWAP CALL LONG ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                                self.entry_symbol_call = vwap_row.symbol
                                if self.entry_symbol_call:
                                    entry_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                    entry_tick = entry_tick.get('c') if entry_tick else np.nan
                                    entry_signal = self.place_trade(current_timestamp, self.entry_symbol_call, entry_tick, 65, 65*entry_tick, "BUY", "ENTRY_LONG")
                                    self.in_position_call = 1
                                    self.signal_list.append(entry_signal)
                                else:
                                    print(f'Call Symbol Is Empty. Skipping.')
                                    self.in_position_call = 0
                        
                        if vwap_row.c < vwap_row.vwap and self.in_position_call != -1:
                            current_timestamp = str(vwap_row.date) + '' + str(vwap_row.time)
                            if self.in_position_call == 1:
                                print(f'VWAP CALL LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                                if self.entry_symbol_call:
                                    exit_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                    exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                    exit_signal = self.place_trade(current_timestamp, self.entry_symbol_call, exit_tick, 65, 65*exit_tick, 'SELL', 'EXIT_LONG')
                                    self.signal_list.append(exit_signal)
                                    self.in_position_call = 0
                                else:
                                    print(f'Call Symbol Is Empty. Skipping.')
                                    self.in_position_call = 0
                            
                                print(f'VWAP CALL SHORT ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                                self.entry_symbol_call = vwap_row.symbol
                                if self.entry_symbol_call:
                                    entry_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                    entry_tick = entry_tick.get('c') if entry_tick else np.nan
                                    entry_signal = self.place_trade(current_timestamp, self.entry_symbol_call, entry_tick, 65, 65*entry_tick, "SHORT", "ENTRY_SHORT")
                                    self.in_position_call = -1
                                    self.signal_list.append(entry_signal)
                                else:
                                    print(f'Call Symbol Is Empty. Skipping.')
                                    self.in_position_call = 0

                            elif self.in_position_call == 0:
                                print(f'VWAP CALL SHORT ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                                self.entry_symbol_call = vwap_row.symbol
                                if self.entry_symbol_call:
                                    entry_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                    entry_tick = entry_tick.get('c') if entry_tick else np.nan
                                    entry_signal = self.place_trade(current_timestamp, self.entry_symbol_call, entry_tick, 65, 65*entry_tick, "SHORT", "ENTRY_SHORT")
                                    self.in_position_call = -1
                                    self.signal_list.append(entry_signal)
                                else:
                                    print(f'Call Symbol Is Empty. Skipping.')
                                    self.in_position_call = 0

                    if vwap_row.time >= self.exit_condition_time:
                        current_timestamp = str(vwap_row.date) + '' + str(vwap_row.time)
                        if self.in_position_call == -1:
                            print(f'VWAP CALL SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                            if self.entry_symbol_call:
                                exit_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                exit_signal = self.place_trade(current_timestamp, self.entry_symbol_call, exit_tick, 65, 65*exit_tick, 'COVER', 'EXIT_SHORT')
                                self.signal_list.append(exit_signal)
                                self.in_position_call = 0
                            else:
                                print(f'Call Symbol Is Empty. Skipping.')
                                self.in_position_call = 0
                        
                        if self.in_position_call == 1:
                            print(f'VWAP CALL LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                            if self.entry_symbol_call:
                                exit_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                exit_signal = self.place_trade(current_timestamp, self.entry_symbol_call, exit_tick, 65, 65*exit_tick, 'SELL', 'EXIT_LONG')
                                self.signal_list.append(exit_signal)
                                self.in_position_call = 0
                            else:
                                print(f'Call Symbol Is Empty. Skipping.')
                                self.in_position_call = 0

    
    def gen_signals_put(self, row):

        if row.time == self.start:
            print(f'Found Valid Time For DateTime: {row.date} {row.time}')
            put_df = self.get_relevant_options_dataframes(row.date, row.time, 'PE', row.c)
            if not put_df.empty and put_df is not None:
                vwap_df = self.calculate_vwap(put_df)
                put_vwap_itertuples = self.create_itertuples(vwap_df)
            
                for vwap_row in put_vwap_itertuples:
                    if vwap_row.time >= self.entry_condition_time and vwap_row.time < self.exit_condition_time:
                        if vwap_row.c > vwap_row.vwap and self.in_position_put != 1:
                            current_timestamp = str(vwap_row.date) + '' + str(vwap_row.time)
                            if self.in_position_put == -1:
                                print(f'VWAP PUT SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                                if self.entry_symbol_put:
                                    exit_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                    exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                    exit_signal = self.place_trade(current_timestamp, self.entry_symbol_put, exit_tick, 65, 65*exit_tick, 'COVER', 'EXIT_SHORT')
                                    self.signal_list.append(exit_signal)
                                    self.in_position_put = 0
                                else:
                                    print(f'Put Symbol Is Empty. Skipping.')
                                    self.in_position_put = 0
                            
                                print(f'VWAP PUT LONG ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                                self.entry_symbol_put = vwap_row.symbol
                                if self.entry_symbol_put:
                                    entry_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                    entry_tick = entry_tick.get('c') if entry_tick else np.nan
                                    entry_signal = self.place_trade(current_timestamp, self.entry_symbol_put, entry_tick, 65, 65*entry_tick, "BUY", "ENTRY_LONG")
                                    self.in_position_put = 1
                                    self.signal_list.append(entry_signal)
                                else:
                                    print(f'Put Symbol Is Empty. Skipping.')
                                    self.in_position_put = 0

                            elif self.in_position_put == 0:
                                print(f'VWAP PUT LONG ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                                self.entry_symbol_put = vwap_row.symbol
                                if self.entry_symbol_put:
                                    entry_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                    entry_tick = entry_tick.get('c') if entry_tick else np.nan
                                    entry_signal = self.place_trade(current_timestamp, self.entry_symbol_put, entry_tick, 65, 65*entry_tick, "BUY", "ENTRY_LONG")
                                    self.in_position_put = 1
                                    self.signal_list.append(entry_signal)
                                else:
                                    print(f'Put Symbol Is Empty. Skipping.')
                                    self.in_position_put = 0
                        
                        if vwap_row.c < vwap_row.vwap and self.in_position_put != -1:
                            current_timestamp = str(vwap_row.date) + '' + str(vwap_row.time)
                            if self.in_position_put == 1:
                                print(f'VWAP PUT LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                                if self.entry_symbol_put:
                                    exit_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                    exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                    exit_signal = self.place_trade(current_timestamp, self.entry_symbol_put, exit_tick, 65, 65*exit_tick, 'SELL', 'EXIT_LONG')
                                    self.signal_list.append(exit_signal)
                                    self.in_position_put = 0
                                else:
                                    print(f'Put Symbol Is Empty. Skipping.')
                                    self.in_position_put = 0
                            
                                print(f'VWAP PUT SHORT ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                                self.entry_symbol_put = vwap_row.symbol
                                if self.entry_symbol_put:
                                    entry_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                    entry_tick = entry_tick.get('c') if entry_tick else np.nan
                                    entry_signal = self.place_trade(current_timestamp, self.entry_symbol_put, entry_tick, 65, 65*entry_tick, "SHORT", "ENTRY_SHORT")
                                    self.in_position_put = -1
                                    self.signal_list.append(entry_signal)
                                else:
                                    print(f'Put Symbol Is Empty. Skipping.')
                                    self.in_position_put = 0

                            elif self.in_position_put == 0:
                                print(f'VWAP PUT SHORT ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                                self.entry_symbol_put = vwap_row.symbol
                                if self.entry_symbol_put:
                                    entry_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                    entry_tick = entry_tick.get('c') if entry_tick else np.nan
                                    entry_signal = self.place_trade(current_timestamp, self.entry_symbol_put, entry_tick, 65, 65*entry_tick, "SHORT", "ENTRY_SHORT")
                                    self.in_position_put = -1
                                    self.signal_list.append(entry_signal)
                                else:
                                    print(f'Put Symbol Is Empty. Skipping.')
                                    self.in_position_put = 0

                    if vwap_row.time >= self.exit_condition_time:
                        current_timestamp = str(vwap_row.date) + '' + str(vwap_row.time)
                        if self.in_position_put == -1:
                            print(f'VWAP PUT SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                            if self.entry_symbol_put:
                                exit_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                exit_signal = self.place_trade(current_timestamp, self.entry_symbol_put, exit_tick, 65, 65*exit_tick, 'COVER', 'EXIT_SHORT')
                                self.signal_list.append(exit_signal)
                                self.in_position_put = 0
                            else:
                                print(f'Put Symbol Is Empty. Skipping.')
                                self.in_position_put = 0
                        
                        if self.in_position_put == 1:
                            print(f'VWAP PUT LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                            if self.entry_symbol_put:
                                exit_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                exit_signal = self.place_trade(current_timestamp, self.entry_symbol_put, exit_tick, 65, 65*exit_tick, 'SELL', 'EXIT_LONG')
                                self.signal_list.append(exit_signal)
                                self.in_position_put = 0
                            else:
                                print(f'Put Symbol Is Empty. Skipping.')
                                self.in_position_put = 0


    def gen_signals(self):
        spot_df = self.get_spot_df(self.underlying)
        spot_itertuples = self.create_itertuples(spot_df)
        for row in spot_itertuples:
            self.gen_signals_call(row)
            self.gen_signals_put(row)
        
        return self.signal_list

    def run_backtest(self, uid: str):
        self.set_params_from_uid(uid)
        signals = self.gen_signals()
        signals_df = pd.DataFrame(signals)
        tradesheet = self.calc.calculate_pl_in_opt_tradesheet(signals_df)
        tradesheet.to_parquet(f"C:/Users/Prahlad/108_research/tradesheets/vwap/{uid}.parquet")
        print('###########################BACKTEST COMPPLETE################################')
        


