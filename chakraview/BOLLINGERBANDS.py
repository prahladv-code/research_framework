from chakraview.chakraview import ChakraView
import pandas as pd
import numpy as np
import datetime
from chakraview.config import sessions, lot_sizes, strike_diff
from chakraview.logger import logger
from analysis.calculate_metrics import CalculateMetrics
class BOLLINGER(ChakraView):
    def __init__(self):
        super().__init__()
        self.reset_all_variables()
        self.commodities_underlyings = ["GOLD", "CRUDEOIL"]
        self.signals = []
        self.newday = None
        self.calc = CalculateMetrics()
    
    def reset_all_variables(self):
        self.entry_symbol = None
        self.expiry = None
        self.target_price = None
        self.stoploss_level = None
        self.high_level = None
        self.low_level = None
        self.expiry_date = None
        self.in_position = 0
    
    def set_params_from_uid(self, uid: str):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.expiry_code = int(uid_split.pop(0))
        self.timeframe = int(uid_split.pop(0))
        self.ma_period = int(uid_split.pop(0))
        self.std_multiplier = float(uid_split.pop(0))
        self.target = float(uid_split.pop(0))
        self.start_time = sessions.get(self.underlying).get('start')
        self.end_time = sessions.get(self.underlying).get('end')
        self.qty = lot_sizes.get(self.underlying)
        self.strike_diff = strike_diff.get(self.underlying)
    
    def _create_itertuples(self, db):
        return db.itertuples(index = False)
    
    def resample_df(self, db):

        df = db.copy()

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
                'c': 'last'
            })

            resampled_list.append(resampled)

        resampled_df = pd.concat(resampled_list)

        resampled_df = resampled_df.reset_index(drop=False)
        resampled_df['date'] = resampled_df['timestamp'].dt.date
        resampled_df['time'] = resampled_df['timestamp'].dt.time

        return resampled_df.dropna()
    
    def calculate_bollinger_bands(self, db):
        db['ma'] = db['c'].rolling(self.ma_period).mean()
        db['std'] = db['c'].rolling(self.ma_period).std()
        db['bbandtop'] = db['ma'] + self.std_multiplier * db['std']
        db['bbandbot'] = db['ma'] - self.std_multiplier * db['std']
        return db
    
    def get_resampled_options_tick(self, symbol: str, date: datetime.date, time: datetime.time):
        timestamp_offset = self.timeframe - 1
        base_dt = datetime.datetime.combine(date, time)
        if time == datetime.time(15, 15):
            adjusted_timestamp = base_dt + datetime.timedelta(minutes=14)
        else:
            adjusted_timestamp = base_dt + datetime.timedelta(minutes = timestamp_offset)
        
        tick = self.get_tick(symbol, adjusted_timestamp.date(), adjusted_timestamp.time())
        if tick:
            return tick
        else:
            return {}
    
    def find_resampled_options_ticker_by_moneyness(self, underlying, expiry_code, date, time, underlying_price, strike_difference, right, moneyness):
        timestamp_offset = self.timeframe - 1
        base_dt = datetime.datetime.combine(date, time)

        if time == datetime.time(15, 15):
            adjusted_timestamp = base_dt + datetime.timedelta(minutes=14)
        else:
            adjusted_timestamp = base_dt + datetime.timedelta(minutes = timestamp_offset)

        tick = self.find_ticker_by_moneyness(underlying, expiry_code, adjusted_timestamp.date(), adjusted_timestamp.time(), underlying_price, strike_difference, right, moneyness)
        if tick:
            return tick
        else:
            return {}

    def check_newday(self, date: datetime.date):

        if date == self.newday:
            return False
        if date != self.newday:
            self.newday = date
            return True

    def gen_signals(self, row):
        
        newday = self.check_newday(row.date)

        if newday:
            if self.in_position == 0:
                self.reset_all_variables()
        
        if row.c > row.bbandtop:
            if self.in_position == 0:
                if self.low_level is None and self.high_level is None:
                    self.low_level = row.l
                    self.stoploss_level = row.h
        
        elif row.c < row.bbandbot:
            if self.in_position == 0:
                if self.high_level is None and self.low_level is None:
                    self.high_level = row.h
                    self.stoploss_level = row.l
        
        if self.high_level is not None:
            if self.in_position == 0:
                if row.c > self.high_level:
                    entry_ticker = self.find_resampled_options_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, row.time, row.c, self.strike_diff, 'CE', 0)
                    current_timestamp = f"{row.date} {row.time}"
                    if entry_ticker:
                        self.entry_symbol = entry_ticker.get('symbol')
                        entry_price = entry_ticker.get('c')
                        self.expiry = entry_ticker.get('expiry')
                        entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'BUY', 'ENTRY_LONG')
                        self.target_price = entry_price * self.target
                        self.signals.append(entry_trade)
                        self.in_position = 1
                        logger.info(f"CALL LONG SIGNAL FOUND AT {current_timestamp}")
                    else:
                        logger.warning("CALL ENTRY TICKER IS NONE")
                        self.reset_all_variables()
        
        if self.low_level is not None:
            if self.in_position == 0:
                if row.c < self.low_level:
                    entry_ticker = self.find_resampled_options_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, row.time, row.c, self.strike_diff, 'PE', 0)
                    current_timestamp = f"{row.date} {row.time}"
                    if entry_ticker:
                        self.entry_symbol = entry_ticker.get('symbol')
                        entry_price = entry_ticker.get('c')
                        self.expiry == entry_ticker.get('expiry')
                        entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'BUY', 'ENTRY_LONG')
                        self.target_price = entry_price * self.target
                        self.signals.append(entry_trade)
                        self.in_position = -1
                        logger.info(f"PUT LONG SIGNAL FOUND AT {current_timestamp}")
                    else:
                        logger.warning("PUT ENTRY TICKER IS NONE")
                        self.reset_all_variables()
        
        if self.stoploss_level is not None:
            if self.in_position == 1:
                if row.c < self.stoploss_level:
                    current_timestamp = f"{row.date} {row.time}"
                    if self.entry_symbol:
                        exit_tick = self.get_resampled_options_tick(self.entry_symbol, row.date, row.time)
                    else:
                        exit_tick = None
                    if exit_tick:
                        exit_price = exit_tick.get('c')
                        exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'SELL', 'EXIT_LONG')
                        self.signals.append(exit_trade)
                        self.reset_all_variables()
                        logger.info(f'LONG CALL EXIT SIGNAL FOUND AT {current_timestamp}')
                    else:
                        logger.warning("CALL EXIT TICK FOUND EMPTY. SKIPPING.")
                        self.reset_all_variables()

            if self.in_position == -1:
                if row.c > self.stoploss_level:
                    current_timestamp = f"{row.date} {row.time}"
                    if self.entry_symbol:
                        exit_tick = self.get_resampled_options_tick(self.entry_symbol, row.date, row.time)
                    else:
                        exit_tick = None
                    if exit_tick:
                        exit_price = exit_tick.get('c')
                        exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'SELL', 'EXIT_LONG')
                        self.signals.append(exit_trade)
                        self.reset_all_variables()
                        logger.info(f'LONG PUT EXIT SIGNAL FOUND AT {current_timestamp}')
                    else:
                        logger.warning("PUT EXIT TICK FOUND EMPTY. SKIPPING.")
                        self.reset_all_variables()
        
        if self.in_position == 1:
            if self.target_price is not None:
                target_ticker = self.get_resampled_options_tick(self.entry_symbol, row.date, row.time)
                if target_ticker:
                    target_tick = target_ticker.get('c')
                    if target_tick >= self.target_price:
                        current_timestamp = f"{row.date} {row.time}"
                        exit_price = target_tick
                        exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'SELL', 'EXIT_LONG')
                        self.signals.append(exit_trade)
                        self.reset_all_variables()
                        logger.info(f'LONG CALL EXIT SIGNAL FOUND AT {current_timestamp}')
                else:
                    logger.warning(f'CALL EXIT TICK FOUND EMPTY. SKIPPING.')
                    self.reset_all_variables()
        
        if self.in_position == -1:
            if self.target_price is not None:
                target_ticker = self.get_resampled_options_tick(self.entry_symbol, row.date, row.time)
                if target_ticker:
                    target_tick = target_ticker.get('c')
                    if target_tick >= self.target_price:
                        current_timestamp = f"{row.date} {row.time}"
                        exit_price = target_tick
                        exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'SELL', 'EXIT_LONG')
                        self.signals.append(exit_trade)
                        self.reset_all_variables()
                        logger.info(f'LONG PUT EXIT SIGNAL FOUND AT {current_timestamp}')
                else:
                    logger.warning("PUT EXIT TICK FOUND EMPTY. SKIPPING.")
                    self.reset_all_variables()

        if row.date == self.expiry and row.time == datetime.time(15, 5):
            if self.in_position == 1:
                exit_ticker = self.get_resampled_options_tick(self.entry_symbol, row.date, row.time)
                if exit_ticker:
                    current_timestamp = f"{row.date} {row.time}"
                    exit_price = exit_ticker.get('c')
                    exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, exit_price * self.qty, 'SELL', 'EXIT_LONG')
                    self.signals.append(exit_trade)
                    self.reset_all_variables()
                    logger.info(f'LONG CALL EXIT SIGNAL FOUND AT {current_timestamp}')
                else:
                    logger.warning(f'CALL EXIT TICK FOUND EMPTY. SKIPPING.')
                    self.reset_all_variables()

            if self.in_position == -1:
                exit_ticker = self.get_resampled_options_tick(self.entry_symbol, row.date, row.time)
                if exit_ticker:
                    current_timestamp = f"{row.date} {row.time}"
                    exit_price = exit_ticker.get('c')
                    exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, exit_price * self.qty, 'SELL', 'EXIT_LONG')
                    self.signals.append(exit_trade)
                    self.reset_all_variables()
                    logger.info(f'LONG PUT EXIT SIGNAL FOUND AT {current_timestamp}')
                else:
                    logger.warning(f'PUT EXIT TICK FOUND EMPTY. SKIPPING.')
                    self.reset_all_variables()


    def run_backtest(self, uid: str):
        self.set_params_from_uid(uid)
        if self.underlying in self.commodities_underlyings:
            spot_df = self.get_spot_df(f'{self.commodities_underlyings}_I')
        else:
            spot_df = self.get_spot_df(self.underlying)
        
        spot_df = spot_df.sort_values(by=['date', 'time']).reset_index(drop=True)
        filtered_spot_df = spot_df[(spot_df['time'] >= self.start_time) & (spot_df['time'] <= self.end_time)].copy().reset_index(drop=True)
        resampled_df = self.resample_df(filtered_spot_df)
        bollinger_df = self.calculate_bollinger_bands(resampled_df)
        print(bollinger_df.tail(50))
        spot_itertuples = self._create_itertuples(bollinger_df)
        for row in spot_itertuples:
            self.gen_signals(row)
        
        tradesheet = pd.DataFrame(self.signals)
        tradesheet = self.calc.calculate_pl_in_opt_tradesheet(tradesheet)
        tradesheet.to_parquet(f"C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/bollinger/{uid}.parquet")
        logger.info('###########################BACKTEST COMPLETE################################')


if __name__ == '__main__':
    bb = BOLLINGER()
    bb.run_backtest("BOLLINGERBANDS_NIFTY_0_25_60_2_1.5")