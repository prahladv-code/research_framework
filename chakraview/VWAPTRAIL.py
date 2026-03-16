from chakraview.chakraview import ChakraView
import pandas as pd
import numpy as np
import datetime
import time
from chakraview.config import sessions, lot_sizes, strike_diff
from analysis import calculate_metrics

class VWAP(ChakraView):
    def __init__(self):
        super().__init__()
        self.entry_condition_time = datetime.time(9, 16, 0)
        self.exit_condition_time = datetime.time(15, 27, 0)
        self.signal_list = []
        self.calc = calculate_metrics.CalculateMetrics()
        self.new_day_call = None
        self.new_day_put = None
        self.reset_all_variables_call()
        self.reset_all_variables_put()
    
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
        self.qty = lot_sizes.get(self.underlying)
        self.strike_diff = strike_diff.get(self.underlying)

    def calculate_vwap(self, df):
        """
        Calculate VWAP per day only (resets each day).
        Requires df to be sorted by timestamp before calling.
        """
        df = df.copy()
        df['fair_price'] = (df['h'] + df['l'] + df['c']) / 3
        df['weighted_price'] = df['fair_price'] * df['v']

        # Group by date so VWAP resets each day
        df['total_volume'] = df.groupby('date')['v'].cumsum()
        df['total_weighted_price'] = df.groupby('date')['weighted_price'].cumsum()
        df['vwap'] = df['total_weighted_price'] / df['total_volume']
        return df

    def check_newday_call(self, date):
        if date == self.new_day_call:
            return False
        if date != self.new_day_call:
            self.new_day_call = date
            return True
    
    def check_newday_put(self, date):
        if date == self.new_day_put:
            return False
        if date != self.new_day_put:
            self.new_day_put = date
            return True
        
    def reset_all_variables_call(self):
        self.entry_symbol_call = None
        self.in_position_call = 0
    
    def reset_all_variables_put(self):
        self.in_position_put = 0
        self.entry_symbol_put = None

    def generate_resampled_timestamps(self):
        int_timeframe = int(self.timeframe)
        print(f'TimeFrame Debug: {self.timeframe}')
        self.start_time = datetime.time(9, 15)
        self.end_time = datetime.time(15, 29)
        start_dt = datetime.datetime.combine(datetime.date.today(), self.start_time)
        end_dt = datetime.datetime.combine(datetime.date.today(), self.end_time)
        print(f'Start Time DEBUG: {start_dt}')
        print(f'End Time Debug: {end_dt}')
        adjusted_end_dt = end_dt - datetime.timedelta(minutes=2)
        valid_timestamps = []
        while start_dt <= adjusted_end_dt:
            start_dt += datetime.timedelta(minutes=int_timeframe)
            if start_dt > adjusted_end_dt:
                valid_timestamps.append(adjusted_end_dt.time())
                break
            valid_timestamps.append(start_dt.time())
        return valid_timestamps

    def get_relevant_options_dataframes(self, date: datetime.date, time: datetime.time, right: str, underlying_price: float):
        strike_details = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, date, time, underlying_price, self.strike_diff, right, 0)
        if strike_details:
            symbol = strike_details.get('symbol')
            symbol_df = self.get_all_ticks_by_symbol(symbol)
            symbol_df['date'] = pd.to_datetime(symbol_df['date'], format="%Y-%m-%d").dt.date
            symbol_df['time'] = pd.to_datetime(symbol_df['time'], format="%H:%M:%S").dt.time
            day_df = symbol_df.copy()
            day_df['timestamp'] = pd.to_datetime(day_df['date'].astype(str) + ' ' + day_df['time'].astype(str))

            # Sort by full timestamp across ALL dates before resampling
            day_df.sort_values('timestamp', inplace=True)
            day_df.reset_index(drop=True, inplace=True)

            self.valid_timestamps = self.generate_resampled_timestamps()
            resampled_df = day_df[day_df['timestamp'].dt.time.isin(self.valid_timestamps)].copy()

            # Re-sort after filter to guarantee order
            resampled_df.sort_values('timestamp', inplace=True)
            resampled_df.reset_index(drop=True, inplace=True)

            return resampled_df
        else:
            return pd.DataFrame()

    def create_itertuples(self, db):
        return db.itertuples(index=False)

    def calculate_chandelier_exit(self, db):
        period = int(self.trailing_stop_period)
        db['prev_close'] = db['c'].shift(1)
        db['tr'] = np.maximum.reduce([
            db['h'] - db['l'],
            (db['h'] - db['prev_close']).abs(),
            (db['l'] - db['prev_close']).abs()
        ])
        db['atr'] = db['tr'].rolling(period).mean()
        db['highest_high'] = db['h'].rolling(period).max()
        db['lowest_low'] = db['l'].rolling(period).min()
        db['chandelier_long'] = db['highest_high'] - self.multiplier * db['atr']
        db['chandelier_short'] = db['lowest_low'] + self.multiplier * db['atr']
        return db
    
    def calculate_supertrend(self, db):
        """
        Calculates Supertrend across ALL dates in db (for proper lookback continuity).
        db must be sorted by timestamp before calling.
        """
        try:
            df = db.copy()
            
            prev_close = df['c'].shift(1)
            tr = np.maximum.reduce([
                df['h'] - df['l'],
                (df['h'] - prev_close).abs(),
                (df['l'] - prev_close).abs()
            ])
            
            atr = pd.Series(tr).rolling(self.supertrend_period).mean().to_numpy()
            hl2 = ((df['h'] + df['l']) / 2).to_numpy()
            
            basic_upper = hl2 + self.multiplier * atr
            basic_lower = hl2 - self.multiplier * atr
            
            close = df['c'].to_numpy()
            n = len(df)
            
            final_upper = np.full(n, np.nan)
            final_lower = np.full(n, np.nan)
            supertrend = np.full(n, np.nan)
            trend = np.full(n, 0)
            
            valid_indices = np.where(~np.isnan(atr))[0]
            if len(valid_indices) == 0:
                print('Not enough data to calculate Supertrend.')
                return pd.DataFrame()

            start = valid_indices[0]
            
            final_upper[start] = basic_upper[start]
            final_lower[start] = basic_lower[start]
            
            if close[start] <= final_upper[start]:
                trend[start] = -1
                supertrend[start] = final_upper[start]
            else:
                trend[start] = 1
                supertrend[start] = final_lower[start]
            
            for i in range(start + 1, n):
                if basic_upper[i] < final_upper[i-1] or close[i-1] > final_upper[i-1]:
                    final_upper[i] = basic_upper[i]
                else:
                    final_upper[i] = final_upper[i-1]
                
                if basic_lower[i] > final_lower[i-1] or close[i-1] < final_lower[i-1]:
                    final_lower[i] = basic_lower[i]
                else:
                    final_lower[i] = final_lower[i-1]
                
                if trend[i-1] == 1:
                    trend[i] = -1 if close[i] <= final_lower[i] else 1
                else:
                    trend[i] = 1 if close[i] >= final_upper[i] else -1
                
                supertrend[i] = final_lower[i] if trend[i] == 1 else final_upper[i]
            
            df['final_upper'] = final_upper
            df['final_lower'] = final_lower
            df['supertrend'] = supertrend
            df['trend'] = trend
            return df

        except Exception as e:
            print(f'Error In Calculating Supertrend: {e}')
            return pd.DataFrame()

    def _prepare_combined_df(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """
        Given a raw (all-dates) options dataframe:
          1. Calculate Supertrend across ALL dates (preserves lookback continuity).
          2. Calculate VWAP grouped by date (resets each day).
          3. Return a single sorted dataframe with both indicators merged.
        """
        if raw_df.empty:
            return pd.DataFrame()

        # --- Step 1: Supertrend on full sorted dataframe ---
        supertrend_df = self.calculate_supertrend(raw_df)
        if supertrend_df.empty:
            return pd.DataFrame()

        # --- Step 2: VWAP per day (computed on the supertrend_df which is already sorted) ---
        combined_df = self.calculate_vwap(supertrend_df)

        # --- Step 3: Final sort guarantee ---
        combined_df.sort_values('timestamp', inplace=True)
        combined_df.reset_index(drop=True, inplace=True)

        return combined_df

    def gen_signals_call(self, row):        
        if row.time == self.start:
            print(f'Found Valid Time For DateTime: {row.date} {row.time}')
            call_df = self.get_relevant_options_dataframes(row.date, row.time, 'CE', row.c)

            if call_df is not None and not call_df.empty:
                # Supertrend: all dates. VWAP: per day. Both on same sorted df.
                combined_df = self._prepare_combined_df(call_df)

                if combined_df.empty:
                    print(f'Call Combined DataFrame Is Empty. Skipping.')
                    return

                # Only iterate over the current day's rows for signal generation
                day_df = combined_df[combined_df['date'] == row.date].copy()
                if day_df.empty:
                    print(f'No data for {row.date} in call combined df. Skipping.')
                    return

                call_itertuples = self.create_itertuples(day_df)

                for vwap_row in call_itertuples:

                    newday = self.check_newday_call(vwap_row.date)
                    if newday:
                        self.reset_all_variables_call()

                    if self.entry_condition_time <= vwap_row.time < self.exit_condition_time:
                        if vwap_row.c > vwap_row.vwap:
                            if vwap_row.c > vwap_row.supertrend:
                                if self.in_position_call == -1:                                
                                    current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                                    print(f'VWAP CALL SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                                    if self.entry_symbol_call:
                                        exit_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                        exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                        exit_signal = self.place_trade(current_timestamp, self.entry_symbol_call, exit_tick, self.qty, self.qty*exit_tick, 'COVER', 'EXIT_SHORT')
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
                                        entry_signal = self.place_trade(current_timestamp, self.entry_symbol_call, entry_tick, self.qty, self.qty*entry_tick, "BUY", "ENTRY_LONG")
                                        self.in_position_call = 1
                                        self.signal_list.append(entry_signal)
                                    else:
                                        print(f'Call Symbol Is Empty. Skipping.')
                                        self.in_position_call = 0

                                elif self.in_position_call == 0:
                                    print(f'VWAP CALL LONG ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                                    current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                                    self.entry_symbol_call = vwap_row.symbol
                                    if self.entry_symbol_call:
                                        entry_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                        entry_tick = entry_tick.get('c') if entry_tick else np.nan
                                        entry_signal = self.place_trade(current_timestamp, self.entry_symbol_call, entry_tick, self.qty, self.qty*entry_tick, "BUY", "ENTRY_LONG")
                                        self.in_position_call = 1
                                        self.signal_list.append(entry_signal)
                                    else:
                                        print(f'Call Symbol Is Empty. Skipping.')
                                        self.in_position_call = 0

                            if vwap_row.c < vwap_row.supertrend:
                                if self.in_position_call == 1:
                                    current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                                    print(f'VWAP CALL LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                                    if self.entry_symbol_call:
                                        exit_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                        exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                        exit_signal = self.place_trade(current_timestamp, self.entry_symbol_call, exit_tick, self.qty, self.qty*exit_tick, 'SELL', 'EXIT_LONG')
                                        self.signal_list.append(exit_signal)
                                        self.in_position_call = 0
                                    else:
                                        print(f'Call Symbol Is Empty. Skipping.')
                                        self.in_position_call = 0

                        if vwap_row.c < vwap_row.vwap:
                            if vwap_row.c < vwap_row.supertrend:
                                if self.in_position_call == 1:
                                    current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                                    print(f'VWAP CALL LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                                    if self.entry_symbol_call:
                                        exit_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                        exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                        exit_signal = self.place_trade(current_timestamp, self.entry_symbol_call, exit_tick, self.qty, self.qty*exit_tick, 'SELL', 'EXIT_LONG')
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
                                        entry_signal = self.place_trade(current_timestamp, self.entry_symbol_call, entry_tick, self.qty, self.qty*entry_tick, "SHORT", "ENTRY_SHORT")
                                        self.in_position_call = -1
                                        self.signal_list.append(entry_signal)
                                    else:
                                        print(f'Call Symbol Is Empty. Skipping.')
                                        self.in_position_call = 0

                                elif self.in_position_call == 0:
                                    print(f'VWAP CALL SHORT ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                                    current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                                    self.entry_symbol_call = vwap_row.symbol
                                    if self.entry_symbol_call:
                                        entry_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                        entry_tick = entry_tick.get('c') if entry_tick else np.nan
                                        entry_signal = self.place_trade(current_timestamp, self.entry_symbol_call, entry_tick, self.qty, self.qty*entry_tick, "SHORT", "ENTRY_SHORT")
                                        self.in_position_call = -1
                                        self.signal_list.append(entry_signal)
                                    else:
                                        print(f'Call Symbol Is Empty. Skipping.')
                                        self.in_position_call = 0

                            if vwap_row.c > vwap_row.supertrend:
                                if self.in_position_call == -1:
                                    current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                                    print(f'VWAP CALL SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                                    if self.entry_symbol_call:
                                        exit_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                        exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                        exit_signal = self.place_trade(current_timestamp, self.entry_symbol_call, exit_tick, self.qty, self.qty*exit_tick, 'COVER', 'EXIT_SHORT')
                                        self.signal_list.append(exit_signal)
                                        self.in_position_call = 0
                                    else:
                                        print(f'Call Symbol Is Empty. Skipping.')
                                        self.in_position_call = 0

                    if vwap_row.time >= self.exit_condition_time:
                        current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                        if self.in_position_call == -1:
                            print(f'VWAP CALL SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                            if self.entry_symbol_call:
                                exit_tick = self.get_tick(self.entry_symbol_call, vwap_row.date, vwap_row.time)
                                exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                exit_signal = self.place_trade(current_timestamp, self.entry_symbol_call, exit_tick, self.qty, self.qty*exit_tick, 'COVER', 'EXIT_SHORT')
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
                                exit_signal = self.place_trade(current_timestamp, self.entry_symbol_call, exit_tick, self.qty, self.qty*exit_tick, 'SELL', 'EXIT_LONG')
                                self.signal_list.append(exit_signal)
                                self.in_position_call = 0
                            else:
                                print(f'Call Symbol Is Empty. Skipping.')
                                self.in_position_call = 0

    def gen_signals_put(self, row):
        if row.time == self.start:
            print(f'Found Valid Time For DateTime: {row.date} {row.time}')
            put_df = self.get_relevant_options_dataframes(row.date, row.time, 'PE', row.c)

            if put_df is not None and not put_df.empty:
                # Supertrend: all dates. VWAP: per day. Both on same sorted df.
                combined_df = self._prepare_combined_df(put_df)

                if combined_df.empty:
                    print(f'Put Combined DataFrame Is Empty. Skipping.')
                    return

                # Only iterate over the current day's rows for signal generation
                day_df = combined_df[combined_df['date'] == row.date].copy()
                if day_df.empty:
                    print(f'No data for {row.date} in put combined df. Skipping.')
                    return

                put_itertuples = self.create_itertuples(day_df)

                for vwap_row in put_itertuples:

                    newday = self.check_newday_put(vwap_row.date)
                    if newday:
                        self.reset_all_variables_put()
                        
                    if self.entry_condition_time <= vwap_row.time < self.exit_condition_time:
                        if vwap_row.c > vwap_row.vwap:
                            if vwap_row.c > vwap_row.supertrend:
                                if self.in_position_put == -1:
                                    current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                                    print(f'VWAP PUT SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                                    if self.entry_symbol_put:
                                        exit_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                        exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                        exit_signal = self.place_trade(current_timestamp, self.entry_symbol_put, exit_tick, self.qty, self.qty*exit_tick, 'COVER', 'EXIT_SHORT')
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
                                        entry_signal = self.place_trade(current_timestamp, self.entry_symbol_put, entry_tick, self.qty, self.qty*entry_tick, "BUY", "ENTRY_LONG")
                                        self.in_position_put = 1
                                        self.signal_list.append(entry_signal)
                                    else:
                                        print(f'Put Symbol Is Empty. Skipping.')
                                        self.in_position_put = 0

                                elif self.in_position_put == 0:
                                    current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                                    print(f'VWAP PUT LONG ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                                    self.entry_symbol_put = vwap_row.symbol
                                    if self.entry_symbol_put:
                                        entry_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                        entry_tick = entry_tick.get('c') if entry_tick else np.nan
                                        entry_signal = self.place_trade(current_timestamp, self.entry_symbol_put, entry_tick, self.qty, self.qty*entry_tick, "BUY", "ENTRY_LONG")
                                        self.in_position_put = 1
                                        self.signal_list.append(entry_signal)
                                    else:
                                        print(f'Put Symbol Is Empty. Skipping.')
                                        self.in_position_put = 0

                            if vwap_row.c < vwap_row.supertrend:
                                if self.in_position_put == 1:
                                    current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                                    print(f'VWAP PUT LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                                    if self.entry_symbol_put:
                                        exit_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                        exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                        exit_signal = self.place_trade(current_timestamp, self.entry_symbol_put, exit_tick, self.qty, self.qty*exit_tick, 'SELL', 'EXIT_LONG')
                                        self.signal_list.append(exit_signal)
                                        self.in_position_put = 0
                                    else:
                                        print(f'Put Symbol Is Empty. Skipping.')
                                        self.in_position_put = 0

                        if vwap_row.c < vwap_row.vwap:
                            if vwap_row.c < vwap_row.supertrend:
                                if self.in_position_put == 1:
                                    current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                                    print(f'VWAP PUT LONG EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                                    if self.entry_symbol_put:
                                        exit_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                        exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                        exit_signal = self.place_trade(current_timestamp, self.entry_symbol_put, exit_tick, self.qty, self.qty*exit_tick, 'SELL', 'EXIT_LONG')
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
                                        entry_signal = self.place_trade(current_timestamp, self.entry_symbol_put, entry_tick, self.qty, self.qty*entry_tick, "SHORT", "ENTRY_SHORT")
                                        self.in_position_put = -1
                                        self.signal_list.append(entry_signal)
                                    else:
                                        print(f'Put Symbol Is Empty. Skipping.')
                                        self.in_position_put = 0

                                elif self.in_position_put == 0:
                                    current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                                    print(f'VWAP PUT SHORT ENTRY SIGNAL FOUND AT {vwap_row.date} {vwap_row.time}')
                                    self.entry_symbol_put = vwap_row.symbol
                                    if self.entry_symbol_put:
                                        entry_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                        entry_tick = entry_tick.get('c') if entry_tick else np.nan
                                        entry_signal = self.place_trade(current_timestamp, self.entry_symbol_put, entry_tick, self.qty, self.qty*entry_tick, "SHORT", "ENTRY_SHORT")
                                        self.in_position_put = -1
                                        self.signal_list.append(entry_signal)
                                    else:
                                        print(f'Put Symbol Is Empty. Skipping.')
                                        self.in_position_put = 0

                            if vwap_row.c > vwap_row.supertrend:
                                if self.in_position_put == -1:
                                    current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                                    print(f'VWAP PUT SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                                    if self.entry_symbol_put:
                                        exit_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                        exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                        exit_signal = self.place_trade(current_timestamp, self.entry_symbol_put, exit_tick, self.qty, self.qty*exit_tick, 'COVER', 'EXIT_SHORT')
                                        self.signal_list.append(exit_signal)
                                        self.in_position_put = 0
                                    else:
                                        print(f'Put Symbol Is Empty. Skipping.')
                                        self.in_position_put = 0

                    if vwap_row.time >= self.exit_condition_time:
                        current_timestamp = f"{vwap_row.date} {vwap_row.time}"
                        if self.in_position_put == -1:
                            print(f'VWAP PUT SHORT EXIT FOUND AT {vwap_row.date} {vwap_row.time}')
                            if self.entry_symbol_put:
                                exit_tick = self.get_tick(self.entry_symbol_put, vwap_row.date, vwap_row.time)
                                exit_tick = exit_tick.get('c') if exit_tick else np.nan
                                exit_signal = self.place_trade(current_timestamp, self.entry_symbol_put, exit_tick, self.qty, self.qty*exit_tick, 'COVER', 'EXIT_SHORT')
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
                                exit_signal = self.place_trade(current_timestamp, self.entry_symbol_put, exit_tick, self.qty, self.qty*exit_tick, 'SELL', 'EXIT_LONG')
                                self.signal_list.append(exit_signal)
                                self.in_position_put = 0
                            else:
                                print(f'Put Symbol Is Empty. Skipping.')
                                self.in_position_put = 0

    def gen_signals(self):
        commodities = ['GOLD', 'CRUDEOIL', 'SILVER']
        commodities_underlying = None
        if self.underlying in commodities:
            commodities_underlying = f'{self.underlying}_I'
        else:
            commodities_underlying = None
            
        if commodities_underlying is not None:
            spot_df = self.get_spot_df(commodities_underlying)
        else:
            spot_df = self.get_spot_df(self.underlying)
        
        spot_df = spot_df.drop_duplicates(subset=['date', 'time'])
        spot_df.sort_values(['date', 'time'], inplace=True)
        spot_df.reset_index(drop=True, inplace=True)
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
        # tradesheet.to_csv(f"C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/vwaptrail/{uid}.csv")
        tradesheet.to_parquet(f"C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/vwaptrail/{uid}.parquet")
        print('###########################BACKTEST COMPLETE################################')