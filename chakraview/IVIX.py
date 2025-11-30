from chakraview import ChakraView
import pandas as pd
import numpy as np
import duckdb
import time
from config import sessions
import datetime
import time
from config import r
import re

class IVIX(ChakraView):
    def __init__(self):
        super().__init__()
        self.start_time = sessions.get('nifty').get('start')
        self.end_time = sessions.get('nifty').get('end')
        self.live_positions = []
        self.position_count = 0
    
    def create_iterable(self, df):
        return df.itertuples()
    
    def set_params_from_uid(self, uid):
        self.uid_split = uid.split('_')
        self.strat = self.uid_split.pop(0)
        self.underlying = self.uid_split.pop(0)
        self.timeframe = self.uid_split.pop(0)
        self.indicator_period = int(self.uid_split.pop(0))
        self.max_entries = self.uid_split.pop(0) == "True"
        self.max_entry_num = self.uid_split.pop(0) if self.max_entries else None
        self.int_timeframe = int(self.timeframe)

    def generate_resampled_df(self):
        self.straddle_df = self.get_df('nifty_straddle')
        self.straddle_df['straddle_price'] = self.straddle_df['straddle_price'].ffill()

        # Combine date + time into a datetime index
        self.straddle_df['datetime'] = self.straddle_df.apply(
            lambda row: pd.Timestamp.combine(row['date'], row['time']),
            axis=1
        )

        # Set datetime as index
        self.straddle_df = self.straddle_df.set_index('datetime')

        # --- OPTIONAL: sort just to be safe ---
        self.straddle_df = self.straddle_df.sort_index()

        # Define your OHLC aggregation rules
        ohlc_dict = {
            'o': 'first',
            'h': 'max',
            'l': 'min',
            'c': 'last',
            'straddle_price': 'last',        # you can also take last if you prefer
        }

        # Apply resampling
        df_resampled = self.straddle_df.resample(f'{self.timeframe}T').agg(ohlc_dict).dropna(how='all').reset_index()
        return df_resampled
    

    def generate_straddle_indicator(self, resampled_df):
        resampled_df['new_day'] = resampled_df['datetime'].dt.date != resampled_df['datetime'].shift().dt.date
        resampled_df['first_straddle_price'] = np.where(resampled_df['new_day'], resampled_df['straddle_price'], np.nan)
        resampled_df['roc'] = np.where(resampled_df['first_straddle_price'].notna(), resampled_df['straddle_price']/resampled_df['first_straddle_price'].shift(), resampled_df['straddle_price']/resampled_df['straddle_price'].shift())
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

        resampled_df = self.generate_resampled_df()
        indicator_df = self.generate_straddle_indicator(resampled_df)
        signal_iterable = self.create_iterable(indicator_df)
        return signal_iterable
    

    def get_all_condor_tickers(self, date, time, spot):
        self.short_call = self.find_ticker_by_delta(date, time, 0.25, 'CE', spot)
        self.short_put = self.find_ticker_by_delta(date, time, 0.25, 'PE', spot)
        self.protection_call = self.find_ticker_by_delta(date, time, 0.05, 'CE', spot)
        self.protection_put = self.find_ticker_by_delta(date, time, 0.05, 'PE', spot)
        if self.short_call and self.short_put and self.protection_call and self.protection_put:
            return True
        else:
            return False

    
    def calculate_payoff(self):
        protection_call_strike = self.protection_call.get('strike')
        protection_put_strike = self.protection_put.get('strike')
        short_strike_call = self.short_call.get('strike')
        short_strike_put = self.short_put.get('strike')
        short_entry_call = self.short_call.get('c')
        short_entry_put = self.short_put.get('c')
        protection_entry_call = self.protection_call.get('c')
        protection_entry_put = self.protection_put.get('c')
        payoff_list = []
        # starting point = 50 pts below protection put strike
        start_nifty = protection_put_strike - 50

        # end point = you likely want 50 points above protection call
        end_nifty = protection_call_strike + 50

        payoff_list = []

        price = start_nifty
        while price <= end_nifty:
            updated_short_call_price = price - short_strike_call
            updated_short_call_price = updated_short_call_price if updated_short_call_price > 0 else 0
            updated_short_put_price = short_strike_put - price
            updated_short_put_price = updated_short_put_price if updated_short_put_price > 0 else 0
            updated_long_call_price = price - protection_call_strike
            updated_long_call_price = updated_long_call_price if updated_long_call_price > 0 else 0
            updated_long_put_price  = protection_put_strike - price
            updated_long_put_price = updated_long_put_price if updated_long_put_price > 0 else 0

            short_call_pl = (short_entry_call - updated_short_call_price) * 75
            long_call_pl = (updated_long_call_price - protection_entry_call) * 75
            short_put_pl = (short_entry_put - updated_short_put_price) * 75
            long_put_pl = (updated_long_put_price - protection_entry_put) * 75
            total_pl = short_call_pl + long_call_pl + short_put_pl + long_put_pl
            payoff_list.append({'underlying': price, 'P/L': total_pl})

            price += 1      

        payoff_df = pd.DataFrame(payoff_list)
        payoff_df["sign"] = payoff_df["P/L"].apply(lambda x: 1 if x >= 0 else -1)
        payoff_df["sign_shift"] = payoff_df["sign"].shift()
        breakevens = []
        for i in range(1, len(payoff_df)):
            if payoff_df.loc[i, "sign"] != payoff_df.loc[i - 1, "sign"]:
                # Linear interpolation for exact break-even point
                x1, y1 = payoff_df.loc[i - 1, ["underlying", "P/L"]]
                x2, y2 = payoff_df.loc[i, ["underlying", "P/L"]]
                be = x1 + (0 - y1) * (x2 - x1) / (y2 - y1)
                breakevens.append(be)
                
        return breakevens
    
    def calculate_expiry(self, symbol):
        match = re.search(r"(\d{2}[A-Z]{3}\d{2})", symbol)
        if match:
            expiry_str = match.group(1)
            expiry_dt = datetime.datetime.strptime(expiry_str, "%d%b%y")  # convert to datetime
            return expiry_dt

    def place_trade(self, timestamp, symbol, price, qty, cv, trade, system_action):
        return {
            'timestamp': timestamp,
            'symbol': symbol,
            'price': price,
            'qty': qty,
            'cv': cv,
            'trade': trade,
            'system_action': system_action
        }

    def generate_signal(self):
        self.signal_list = []
        iterable = self.generate_signal_iterable()
        end_dt = datetime.datetime.combine(datetime.date(2025, 12, 31), self.end_time)
        end_dt -= datetime.timedelta(minutes=(self.int_timeframe - 1))
        self.end_time = end_dt.time()
        for self.row in iterable:
            print(self.row)
            self.time = self.row.datetime.time()
            self.date = self.row.datetime.date()
            # ENTRY
            if self.row.inverse_stochastic_indicator <= 20:
                data_check = self.get_all_condor_tickers(self.date, self.time, self.row.c)
                if data_check:
                    sample_symbol = self.short_call.get('symbol')
                    expiry = self.calculate_expiry(sample_symbol)
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
                            'expiry': expiry
                        }
                    )
                    self.signal_list.append(self.place_trade(self.row.datetime, 
                                                            self.short_call.get('symbol'), 
                                                            self.short_call.get('c'),
                                                            75,
                                                            75*self.short_call.get('c'),
                                                            'SHORT',
                                                            'SHORT_ENTRY'
                                                            ))
                    
                    self.signal_list.append(self.place_trade(self.row.datetime, 
                                                            self.short_put.get('symbol'), 
                                                            self.short_put.get('c'),
                                                            75,
                                                            75*self.short_put.get('c'),
                                                            'SHORT',
                                                            'SHORT_ENTRY'
                                                            ))
                    
                    self.signal_list.append(self.place_trade(self.row.datetime, 
                                                            self.protection_call.get('symbol'), 
                                                            self.protection_call.get('c'),
                                                            75,
                                                            75*self.protection_call.get('c'),
                                                            'LONG',
                                                            'LONG_ENTRY'
                                                            ))
                    
                    self.signal_list.append(self.place_trade(self.row.datetime, 
                                                            self.protection_put.get('symbol'), 
                                                            self.protection_put.get('c'),
                                                            75,
                                                            75*self.protection_put.get('c'),
                                                            'LONG',
                                                            'LONG_ENTRY'
                                                            ))

            #EXITS
            exit_tags = []
            if self.live_positions:
                live_positions_df = pd.DataFrame(self.live_positions)
                for position in live_positions_df.itertuples():
                    print(f'EXPIRY: {position.expiry.date()}')
                    print(f'TODAY {self.date} {self.time}')
                    print(f'END TIME {self.end_time}')
                    if self.row.c >= position.upper_breakeven:
                        exit_tick_short_call = self.get_tick(position.short_call, self.date, self.time)
                        exit_tick_short_put = self.get_tick(position.short_put, self.date, self.time)
                        exit_tick_protection_call = self.get_tick(position.protection_call, self.date, self.time)
                        exit_tick_protection_put = self.get_tick(position.protection_put, self.date, self.time)
                        exit_tags.append(position.system_tag)
                        if exit_tick_short_call and exit_tick_protection_call and exit_tick_protection_put and exit_tick_short_put:
                            
                            self.signal_list.append(self.place_trade(
                                self.row.datetime, position.short_call, exit_tick_short_call.get('c'), 75, 75*exit_tick_short_call.get('c'), 'COVER', 'SHORT_EXIT'
                            ))
                            self.signal_list.append(self.place_trade(
                                self.row.datetime, position.short_put, exit_tick_short_put.get('c'), 75, 75*exit_tick_short_put.get('c'), 'COVER', 'SHORT_EXIT'
                            ))
                            self.signal_list.append(self.place_trade(
                                self.row.datetime, position.protection_call, exit_tick_protection_call.get('c'), 75, 75 * exit_tick_protection_call.get('c'), 'SELL', 'LONG_EXIT'
                            ))
                            self.signal_list.append(self.place_trade(
                                self.row.datetime, position.protection_put, exit_tick_protection_call.get('c'), 75, 75 * exit_tick_protection_put.get('c'), 'SELL', 'LONG_EXIT'
                            ))
                    
                    elif self.row.c <= position.lower_breakeven:
                        exit_tick_short_call = self.get_tick(position.short_call, self.date, self.time)
                        exit_tick_short_put = self.get_tick(position.short_put, self.date, self.time)
                        exit_tick_protection_call = self.get_tick(position.protection_call, self.date, self.time)
                        exit_tick_protection_put = self.get_tick(position.protection_put, self.date, self.time)
                        exit_tags.append(position.system_tag)
                        if exit_tick_short_call and exit_tick_protection_call and exit_tick_protection_put and exit_tick_short_put:
                            self.signal_list.append(self.place_trade(
                                self.row.datetime, position.short_call, exit_tick_short_call.get('c'), 75, 75*exit_tick_short_call.get('c'), 'COVER', 'SHORT_EXIT'
                            ))
                            self.signal_list.append(self.place_trade(
                                self.row.datetime, position.short_put, exit_tick_short_put.get('c'), 75, 75*exit_tick_short_put.get('c'), 'COVER', 'SHORT_EXIT'
                            ))
                            self.signal_list.append(self.place_trade(
                                self.row.datetime, position.protection_call, exit_tick_protection_call.get('c'), 75, 75 * exit_tick_protection_call.get('c'), 'SELL', 'LONG_EXIT'
                            ))
                            self.signal_list.append(self.place_trade(
                                self.row.datetime, position.protection_put, exit_tick_protection_call.get('c'), 75, 75 * exit_tick_protection_put.get('c'), 'SELL', 'LONG_EXIT'
                            ))
                    
                    elif self.date == position.expiry.date() and self.time == self.end_time:
                        print("###################################################CONDOR EXPIRED#################################################")
                        exit_tick_short_call = self.get_tick(position.short_call, self.date, self.time)
                        exit_tick_short_put = self.get_tick(position.short_put, self.date, self.time)
                        exit_tick_protection_call = self.get_tick(position.protection_call, self.date, self.time)
                        exit_tick_protection_put = self.get_tick(position.protection_put, self.date, self.time)
                        exit_tags.append(position.system_tag)
                        if exit_tick_short_call and exit_tick_protection_call and exit_tick_protection_put and exit_tick_short_put:
                            self.signal_list.append(self.place_trade(
                                self.row.datetime, position.short_call, exit_tick_short_call.get('c'), 75, 75*exit_tick_short_call.get('c'), 'COVER', 'SHORT_EXIT'
                            ))
                            self.signal_list.append(self.place_trade(
                                self.row.datetime, position.short_put, exit_tick_short_put.get('c'), 75, 75*exit_tick_short_put.get('c'), 'COVER', 'SHORT_EXIT'
                            ))
                            self.signal_list.append(self.place_trade(
                                self.row.datetime, position.protection_call, exit_tick_protection_call.get('c'), 75, 75 * exit_tick_protection_call.get('c'), 'SELL', 'LONG_EXIT'
                            ))
                            self.signal_list.append(self.place_trade(
                                self.row.datetime, position.protection_put, exit_tick_protection_call.get('c'), 75, 75 * exit_tick_protection_put.get('c'), 'SELL', 'LONG_EXIT'
                            ))

                
                if exit_tags:
                    self.live_positions = [
                        pos for pos in self.live_positions
                        if pos['system_tag'] not in exit_tags
                    ]
                    # after exit_tags final removal
                    self.position_count = len(self.live_positions)

        return self.signal_list






                


        





vix = IVIX()
vix.set_params_from_uid('IVIX_nitfty_15_14_False_0')
signal_list = vix.generate_signal()
signal_df = pd.DataFrame(signal_list)
print(signal_df)
signal_df.to_csv(r'C:\Users\admin\Downloads\signals_ivix_init.csv')
