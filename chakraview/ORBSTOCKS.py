from chakraview.chakraview import ChakraView
import pandas as pd
import datetime
import time
from chakraview.config import sessions, lot_sizes, strike_diff
from chakraview.logger import logger
from analysis.calculate_metrics import CalculateMetrics


class ORBSTOCKS(ChakraView):
    def __init__(self):
        super().__init__()
        self.calc = CalculateMetrics()
        self.signals = []
        self.newday = None
        self.qty = 5
        self.trade_taken = False
        self.reset_all_variables()
    
    def reset_all_variables(self):
        self.entry_symbol = None
        self.stoploss_price = None
        self.target_price = None
        self.in_position = 0
        self.high_level = None
        self.low_level = None
        

    def set_params_from_uid(self, uid: str):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.timeframe = int(uid_split.pop(0))
        self.target_multiplier = float(uid_split.pop(0))
        self.orb_range = int(uid_split.pop(0))
        self.re_enetry = uid_split.pop(0) == 'True'
        self.start = sessions.get("NIFTY").get('start')
        self.end = sessions.get('NIFTY').get('end')
    
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

    def get_resampled_timestamp(self, date: datetime.date, time: datetime.time):
        timestamp_offset = self.timeframe - 1
        base_dt = datetime.datetime.combine(date, time)
        if time == datetime.time(15, 15):
            adjusted_timestamp = base_dt + datetime.timedelta(minutes=timestamp_offset)
            logger.debug(f'Adjusted Date And Time Debug: {adjusted_timestamp.date()} {adjusted_timestamp.time()}')
            return adjusted_timestamp.time()
        else:
            adjusted_timestamp = base_dt + datetime.timedelta(minutes = timestamp_offset)
            logger.debug(f'Adjusted Date And Time Debug: {adjusted_timestamp.date()} {adjusted_timestamp.time()}')
            return adjusted_timestamp.time()
        
        
    def check_new_day(self, date: datetime.date):

        if date == self.newday:
            return False
        if date != self.newday:
            self.newday = date
            return True
    
    def calculate_orb_levels(self, high: float, low: float):
        logger.debug(f'Calculating ORB Levels For Underlying: {self.underlying}. High Input: {high} | Low Input: {low}')
        if self.high_level is None:
            logger.debug(f'Updating High From {self.high_level} to {high}')
            self.high_level = high

        if high > self.high_level:
            logger.debug(f'Updating High From {self.high_level} to {high}')
            self.high_level = high
        
        if self.low_level is None:
            logger.debug(f'Updating Low Level From: {self.low_level} to {low}')
            self.low_level = low
        
        if low < self.low_level:
            logger.debug(f'Updating Low Level From: {self.low_level} to {low}')
            self.low_level = low


    def create_itertuples(self, db: pd.DataFrame):
        return db.itertuples(index=False)
    
    def gen_signals(self, row):

        new_day = self.check_new_day(row.date)

        if new_day:
            self.reset_all_variables()
            self.trade_taken = False

        orb_minute = self.orb_range - 1
        orb_timestamp = datetime.datetime.combine(row.date, self.start) + datetime.timedelta(minutes=orb_minute)
        orb_time = orb_timestamp.time()
        adjusted_timestamp = self.get_resampled_timestamp(row.date, row.time)

        if adjusted_timestamp <= orb_time:
            self.calculate_orb_levels(row.h, row.l)
        
        if adjusted_timestamp > orb_time:
            
            if not self.re_enetry:
                if self.trade_taken:
                    return
            
            if self.high_level is None or self.low_level is None:
                return
            
            # ENTRY
            if row.c > self.high_level:
                if self.in_position == 0:
                    self.entry_symbol = self.underlying
                    self.trade_taken = True
                    entry_trade = 'BUY'
                    timestamp = f'{row.date} {row.time}'
                    self.stoploss_price = self.low_level
                    self.target_price = ((self.high_level - self.low_level) * self.target_multiplier) + row.c
                    entry = self.place_trade(timestamp, self.underlying, row.c, self.qty, self.qty * row.c, entry_trade, 'LONG_ENTRY')
                    self.signals.append(entry)
                    logger.info(f'LONG ENTRY TRADE FOUND AT {row.date} {row.time}')
                    self.in_position = 1
            
            if row.c < self.low_level:
                if self.in_position == 0:
                    self.entry_symbol = self.underlying
                    self.trade_taken = True
                    entry_trade = 'SHORT'
                    timestamp = f'{row.date} {row.time}'
                    self.stoploss_price = self.high_level
                    self.target_price = row.c - ((self.high_level - self.low_level) * self.target_multiplier)
                    entry = self.place_trade(timestamp, self.underlying, row.c, self.qty, self.qty * row.c, entry_trade, 'SHORT_ENTRY')
                    self.signals.append(entry)
                    logger.info(f'SHORT ENTRY TRADE FOUND AT {row.date} {row.time}')
                    self.in_position = -1
            
            if self.in_position == 0:
                return
            
            # EXIT
            if self.in_position == 1:
                if row.c < self.stoploss_price:
                    exit_trade = 'SELL'
                    timestamp = f'{row.date} {row.time}'
                    exit = self.place_trade(timestamp, self.entry_symbol, row.c, self.qty, row.c * self.qty, exit_trade, 'LONG_EXIT')
                    self.signals.append(exit)
                    logger.info(f'LONG STOPLOSS SIGNAL FOUND AT {row.date} {row.time}')
                    self.reset_all_variables()
            
            if self.in_position == -1:
                if row.c > self.stoploss_price:
                    exit_trade = 'COVER'
                    timestamp = f'{row.date} {row.time}'
                    exit = self.place_trade(timestamp, self.entry_symbol, row.c, self.qty, row.c * self.qty, exit_trade, 'SHORT_EXIT')
                    logger.info(f'SHORT STOPLOSS SIGNAL FOUND AT {row.date} {row.time}')
                    self.signals.append(exit)
                    self.reset_all_variables()
            
            if self.in_position == 1:
                if row.c > self.target_price:
                    exit_trade = 'SELL'
                    timestamp = f'{row.date} {row.time}'
                    exit = self.place_trade(timestamp, self.entry_symbol, row.c, self.qty, row.c * self.qty, exit_trade, 'LONG_EXIT')
                    self.signals.append(exit)
                    logger.info(f'LONG TARGET SIGNAL FOUND AT {row.date} {row.time}')
                    self.reset_all_variables()

            if self.in_position == -1:
                if row.c < self.target_price:
                    exit_trade = 'COVER'
                    timestamp = f'{row.date} {row.time}'
                    exit = self.place_trade(timestamp, self.entry_symbol, row.c, self.qty, row.c * self.qty, exit_trade, 'SHORT_EXIT')
                    logger.info(f'SHORT TARGET SIGNAL FOUND AT {row.date} {row.time}')
                    self.signals.append(exit)
                    self.reset_all_variables()
            
            # TIME_EXIT
            if adjusted_timestamp == self.end:
                if self.in_position == 1:
                    exit_trade = 'SELL'
                    timestamp = f'{row.date} {row.time}'
                    exit = self.place_trade(timestamp, self.entry_symbol, row.c, self.qty, row.c * self.qty, exit_trade, 'LONG_EXIT')
                    self.signals.append(exit)
                    logger.info(f'LONG TIME SIGNAL FOUND AT {row.date} {row.time}')
                    self.reset_all_variables()

                elif self.in_position == -1:
                    exit_trade = 'COVER'
                    timestamp = f'{row.date} {row.time}'
                    exit = self.place_trade(timestamp, self.entry_symbol, row.c, self.qty, row.c * self.qty, exit_trade, 'SHORT_EXIT')
                    logger.info(f'SHORT TIME SIGNAL FOUND AT {row.date} {row.time}')
                    self.signals.append(exit)
                    self.reset_all_variables()

    def run_backtest(self, uid: str):
        self.set_params_from_uid(uid)
        spot_df = self.get_spot_df(self.underlying)
        spot_df = self.resample_df(spot_df)
        spot_itertuples = self.create_itertuples(spot_df)

        for row in spot_itertuples:
            self.gen_signals(row)
        
        trades = pd.DataFrame(self.signals)
        tradesheet = self.calc.calculate_pl_in_positional_tradesheet(trades)
        tradesheet.to_parquet(f"C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/orbstocks/{uid}.parquet")
        logger.info("+++++++++++++++++++++++++++++++++++++++++++++++BACKTEST COMPLETE+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        
        