from chakraview.chakraview import ChakraView
import pandas as pd
import numpy as np
import datetime
from chakraview.config import sessions, lot_sizes, strike_diff
from chakraview.logger import logger
from analysis.calculate_metrics import CalculateMetrics


class AVWAPV3(ChakraView):
    def __init__(self):
        super().__init__()
        self.signal_list = []
        self.calc = CalculateMetrics()
        self.reset_all_variables()
        self.logger = logger
    
    def reset_all_variables(self):
        self.in_position = 0
        self.entry_symbol = None
        self.expiry = None
    
    # 'AVWAP_NIFTY_25_W_0_0'
    def set_params_from_uid(self, uid: str):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.timeframe = int(uid_split.pop(0))
        self.avwap_anchor = uid_split.pop(0).lower()
        self.expiry_code = int(uid_split.pop(0))
        self.moneyness = int(uid_split.pop(0))
        self.qty = lot_sizes.get(self.underlying)
        self.strike_diff = strike_diff.get(self.underlying)
        self.start = sessions.get(self.underlying).get('start')
        self.end = sessions.get(self.underlying).get('end')
    
    def resample_df(self, db: pd.DataFrame) -> pd.DataFrame:

        df = db[(db['time'] >= self.start) & (db['time'] <= self.end)].copy()

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
                'v': 'sum'
            })

            resampled_list.append(resampled)

        resampled_df = pd.concat(resampled_list)

        resampled_df = resampled_df.reset_index(drop=False)
        resampled_df['date'] = resampled_df['timestamp'].dt.date
        resampled_df['time'] = resampled_df['timestamp'].dt.time

        return resampled_df.dropna()
    
    def get_resampled_tick(self, date: datetime.date, time: datetime.time):
        timestamp_offset = self.timeframe - 1
        base_dt = datetime.datetime.combine(date, time)

        if time == datetime.time(15, 5):
            adjusted_timestamp = base_dt + datetime.timedelta(minutes=24)
        else:
            adjusted_timestamp = base_dt + datetime.timedelta(minutes = timestamp_offset)
        
        return adjusted_timestamp.time()
    
    def calculate_avwap(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df.copy()

        # ========= DATETIME =========
        df['datetime'] = pd.to_datetime(
            df['date'].astype(str) + ' ' + df['time'].astype(str)
        )

        df = df.sort_values('datetime').reset_index(drop=True)

        # ========= EXPIRY REGIME =========
        def get_expiry_weekday(date):

            # Thursday expiry till 3 Apr 2025
            if date <= datetime.date(2025, 8, 28):
                return 3

            # Tuesday expiry thereafter
            else:
                return 1

        # ========= BUILD EXPIRY CYCLES =========
        unique_dates = sorted(df['date'].unique())

        date_to_cycle = {}
        cycle = 0

        for current_date in unique_dates:

            date_to_cycle[current_date] = cycle

            expiry_weekday = get_expiry_weekday(current_date)

            # increment AFTER expiry day closes
            if current_date.weekday() == expiry_weekday:
                cycle += 1

        df['expiry_cycle'] = df['date'].map(date_to_cycle)

        # ========= PRICE =========
        tp = (df['h'] + df['l'] + df['c']) / 3
        pv = tp * df['v']

        # ========= RUNNING VWAP INSIDE EACH EXPIRY CYCLE =========
        cum_pv = pv.groupby(df['expiry_cycle']).cumsum()
        cum_vol = df['v'].groupby(df['expiry_cycle']).cumsum()

        df['expiry_vwap'] = cum_pv / cum_vol

        # ========= PREVIOUS CYCLE VWAP CLOSE =========
        cycle_close_vwap = (
            df.groupby('expiry_cycle')['expiry_vwap']
            .last()
        )

        prev_cycle_vwap = cycle_close_vwap.shift(1)

        # ========= FORWARD PROJECT INTO NEXT CYCLE =========
        df['expiry_wrs'] = df['expiry_cycle'].map(prev_cycle_vwap)

        # ========= SAFETY FFILL =========
        df['expiry_wrs'] = df['expiry_wrs'].ffill()

        return df

    def create_itertuples(self, df: pd.DataFrame):
        return df.itertuples(index=False)
    
    def get_relevant_anchor(self, row):
        return row.expiry_wrs
    
    def gen_signals(self, row):
        avwap_level = self.get_relevant_anchor(row)
        current_timestamp = f'{row.date} {row.time}'
        adjusted_timestamp = self.get_resampled_tick(row.date, row.time)
        if row.c > avwap_level:
            if self.in_position == -1:
                self.logger.info(f"CALL SHORT EXIT SIGNAL FOUND AT: {current_timestamp}")
                exit_tick = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                if exit_tick:
                    exit_price = exit_tick['c']
                    exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'COVER', 'SHORT_EXIT')
                    self.signal_list.append(exit_trade)
                    self.reset_all_variables()
                else:
                    self.logger.warning(f'Exit Tick Found Empty At: {current_timestamp}.')
                    self.reset_all_variables()
                
                self.logger.info(f'PUT SHORT ENTRY SIGNAL FOUND AT: {current_timestamp}')
                entry_tick = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'PE', self.moneyness)
                if entry_tick:
                    self.entry_symbol = entry_tick['symbol']
                    self.expiry = entry_tick['expiry'].date()
                    entry_price = entry_tick['c']
                    entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'SHORT', 'SHORT_ENTRY')
                    self.signal_list.append(entry_trade)
                    self.in_position = 1
                else:
                    self.logger.warning(f'Entry Tick Found Empty At {current_timestamp}.')
                    self.reset_all_variables()
                    
            elif self.in_position == 0:
                self.logger.info(f'PUT SHORT ENTRY SIGNAL FOUND AT: {current_timestamp}')
                entry_tick = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'PE', self.moneyness)
                if entry_tick:
                    self.entry_symbol = entry_tick['symbol']
                    self.expiry = entry_tick['expiry'].date()
                    entry_price = entry_tick['c']
                    entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'SHORT', 'SHORT_ENTRY')
                    self.signal_list.append(entry_trade)
                    self.in_position = 1
                else:
                    self.logger.warning(f'Entry Tick Found Empty At {current_timestamp}.')
                    self.reset_all_variables()
        
        if row.c < avwap_level:
            if self.in_position == 1:
                self.logger.info(f"PUT SHORT EXIT SIGNAL FOUND AT: {current_timestamp}")
                exit_tick = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                
                if exit_tick:
                    exit_price = exit_tick['c']
                    exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'COVER', 'SHORT_EXIT')
                    self.signal_list.append(exit_trade)
                    self.reset_all_variables()
                else:
                    self.logger.warning(f'Exit Tick Found Empty At: {current_timestamp}.')
                    self.reset_all_variables()
                
                self.logger.info(f'CALL SHORT ENTRY SIGNAL FOUND AT: {current_timestamp}')
                entry_tick = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'CE', self.moneyness)
                if entry_tick:
                    self.entry_symbol = entry_tick['symbol']
                    self.expiry = entry_tick['expiry'].date()
                    entry_price = entry_tick['c']
                    entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'SHORT', 'SHORT_ENTRY')
                    self.signal_list.append(entry_trade)
                    self.in_position = -1
                else:
                    self.logger.warning(f'Entry Tick Found Empty At {current_timestamp}.')
                    self.reset_all_variables()
                    
            elif self.in_position == 0:
                self.logger.info(f'CALL SHORT ENTRY SIGNAL FOUND AT: {current_timestamp}')
                entry_tick = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'CE', self.moneyness)
                if entry_tick:
                    self.entry_symbol = entry_tick['symbol']
                    self.expiry = entry_tick['expiry'].date()
                    entry_price = entry_tick['c']
                    entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'SHORT', 'SHORT_ENTRY')
                    self.signal_list.append(entry_trade)
                    self.in_position = -1
                else:
                    self.logger.warning(f'Entry Tick Found Empty At {current_timestamp}.')
                    self.reset_all_variables()
                    
        if row.date == self.expiry and row.time == datetime.time(15, 5, 0):
            if self.in_position == -1:
                self.logger.info(f'CALL OPTION EXPIRED########################')
                exit_tick = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                if exit_tick:
                    exit_price = exit_tick['c']
                    exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'COVER', 'SHORT_EXIT')
                    self.signal_list.append(exit_trade)
                    self.reset_all_variables()
                else:
                    self.logger.warning(f'Exit Tick Found Empty At: {current_timestamp}.')
                    self.reset_all_variables()
                    
            elif self.in_position == 1:
                self.logger.info('PUT OPTION EXPIRED#############################')
                exit_tick = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                if exit_tick:
                    exit_price = exit_tick['c']
                    exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'COVER', 'SHORT_EXIT')
                    self.signal_list.append(exit_trade)
                    self.reset_all_variables()
                else:
                    self.logger.warning(f'Exit Tick Found Empty At: {current_timestamp}.')
                    self.reset_all_variables()


    def run_backtest(self, uid: str):
        self.set_params_from_uid(uid)
        spot_df = self.get_fut_df(self.underlying, '0')
        resampled_df = self.resample_df(spot_df)
        avwap_df = self.calculate_avwap(resampled_df)
        iterable = self.create_itertuples(avwap_df)
        for row in iterable:
            self.gen_signals(row)
        
        tradebook = pd.DataFrame(self.signal_list)
        tradesheet = self.calc.calculate_pl_in_opt_tradesheet(tradebook)
        tradesheet.to_parquet(f'C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/avwap/{uid}.parquet')
        self.logger.info('##############################BACKTEST COMPLETE############################')
        
