from chakraview.chakraview import ChakraView
import pandas as pd
import numpy as np
import datetime
from chakraview.config import sessions, lot_sizes, strike_diff
from chakraview.logger import logger
from analysis.calculate_metrics import CalculateMetrics

class INTRADAYPULLBACK(ChakraView):
    def __init__(self):
        super().__init__()
        self.reset_all_variables()
        self.calc = CalculateMetrics()
        self.signals = []
        self.newday = None
        self.day_open = None
    
    def reset_all_variables(self):
        self.in_position = 0
        self.entry_symbol = None
        self.expiry = None
        self.stoploss_level = None
        self.target_level = None
        self.target_points = None
        self.high_level = None
        self.low_level = None

    
    def create_itertuples(self, db: pd.DataFrame):
        return db.itertuples(index=False)
    
    # INTRADAYPULLBACK_NIFTY_15_0.5_2_0_0
    def set_params_from_uid(self, uid: str):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.timeframe = int(uid_split.pop(0))
        self.percentage_thresh = float(uid_split.pop(0))
        self.negative_thresh = 0 - self.percentage_thresh
        self.target_multiplier = float(uid_split.pop(0))
        self.expiry_code = int(uid_split.pop(0))
        self.moneyness = int(uid_split.pop(0))
        self.start = sessions.get(self.underlying).get('start')
        self.end = sessions.get(self.underlying).get('end')
        self.qty = lot_sizes.get(self.underlying)
        self.strike_diff = strike_diff.get(self.underlying)
    
    def check_new_day(self, date: datetime.date):

        if date == self.newday:
            return False
        if date != self.newday:
            self.newday = date
            return True
    
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
    
    def get_resampled_tick(self, date: datetime.date, time: datetime.time):
        timestamp_offset = self.timeframe - 1
        base_dt = datetime.datetime.combine(date, time)

        if time == datetime.time(15, 5):
            adjusted_timestamp = base_dt + datetime.timedelta(minutes=24)
        else:
            adjusted_timestamp = base_dt + datetime.timedelta(minutes = timestamp_offset)
        
        return adjusted_timestamp.time()
    
    def gen_signals(self, row):

        current_timestamp = f'{row.date} {row.time}'
        adjusted_timestamp = self.get_resampled_tick(row.date, row.time)

        newday = self.check_new_day(row.date)

        if newday:
            self.reset_all_variables()
            self.day_open = row.o
            
        
        percentage_change = ((row.c - self.day_open) / self.day_open) * 100

        if percentage_change >= self.percentage_thresh:
            if self.in_position == 0:
                logger.debug(f'PERCENTAGE THRESHOLD MET AT {current_timestamp}')
                self.low_level = row.l
                self.stoploss_level = row.h
                self.target_points = (row.h - row.l) * self.target_multiplier
        
        if percentage_change <= self.negative_thresh:
            if self.in_position == 0:
                logger.debug(f'PERCENTAGE THRESHOLD MET AT: {current_timestamp}')
                self.high_level = row.h
                self.stoploss_level = row.l
                self.target_points = (row.h - row.l) * self.target_multiplier
        
        if self.high_level is not None:
            if self.in_position == 0:
                if row.c > self.high_level:
                    logger.info(f"PUT SHORT SIGNAL FOUND AT: {current_timestamp}")
                    entry_ticker = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'PE', self.moneyness)
                    if entry_ticker:
                        entry_price = entry_ticker['c']
                        self.expiry = entry_ticker['expiry'].date()
                        self.entry_symbol = entry_ticker['symbol']
                        self.target_level = row.c + self.target_points
                        entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'SHORT', 'SHORT_ENTRY')
                        self.signals.append(entry_trade)
                        self.in_position = 1
                    else:
                        logger.warning(f'CALL ENTRY TICKER FOUND EMPTY AT: {current_timestamp}')
                        self.reset_all_variables()
        
        if self.low_level is not None:
            if self.in_position == 0:
                if row.c < self.low_level:
                    logger.info(f'CALL SHORT SIGNAL FOUND AT: {current_timestamp}')
                    entry_ticker = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'CE', self.moneyness)
                    if entry_ticker:
                        entry_price = entry_ticker['c']
                        self.expiry = entry_ticker['expiry'].date()
                        self.entry_symbol = entry_ticker['symbol']
                        self.target_level = row.c - self.target_points
                        entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'SHORT', 'SHORT_ENTRY')
                        self.signals.append(entry_trade)
                        self.in_position = -1
                    else:
                        logger.warning(f'PUT ENTRY TICKER FOUND EMPTY AT: {current_timestamp}')
                        self.reset_all_variables()
        
        # if self.target_level is not None:
        #     if self.in_position == 1:
        #         if row.c >= self.target_level:
        #             logger.info(f'CALL SHORT TARGET SIGNAL FOUND AT: {current_timestamp}')
        #             exit_ticker = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
        #             if exit_ticker:
        #                 exit_price = exit_ticker['c']
        #                 exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, exit_price * self.qty, 'COVER', 'SHORT_EXIT')
        #                 self.signals.append(exit_trade)
        #                 self.reset_all_variables()
        #             else:
        #                 logger.warning(f'EXIT TICKER FOUND EMPTY AT: {current_timestamp}')
        #                 self.reset_all_variables()

        #     elif self.in_position == -1:
        #         if row.c <= self.target_level:
        #             logger.info(f'PUT SHORT TARGET SIGNAL FOUND AT: {current_timestamp}')
        #             exit_ticker = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
        #             if exit_ticker:
        #                 exit_price = exit_ticker['c']
        #                 exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'COVER', 'SHORT_EXIT')
        #                 self.signals.append(exit_trade)
        #                 self.reset_all_variables()
        #             else:
        #                 logger.warning(f'EXIT TICKER FOUND EMPTY AT: {current_timestamp}')
        #                 self.reset_all_variables()
        
        if self.stoploss_level is not None:
            if self.in_position == 1:
                if row.c < self.stoploss_level:
                    logger.info(f'PUT SHORT STOPLOSS SIGNAL FOUND AT: {current_timestamp}')
                    exit_ticker = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                    if exit_ticker:
                        exit_price = exit_ticker['c']
                        exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, exit_price * self.qty, 'COVER', 'SHORT_EXIT')
                        self.signals.append(exit_trade)
                        self.reset_all_variables()
                    else:
                        logger.warning(f'EXIT TICKER FOUND EMPTY AT: {current_timestamp}')
                        self.reset_all_variables()

            elif self.in_position == -1:
                if row.c > self.stoploss_level:
                    logger.info(f'CALL SHORT STOPLOSS SIGNAL FOUND AT: {current_timestamp}')
                    exit_ticker = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                    if exit_ticker:
                        exit_price = exit_ticker['c']
                        exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'COVER', 'SHORT_EXIT')
                        self.signals.append(exit_trade)
                        self.reset_all_variables()
                    else:
                        logger.warning(f'EXIT TICKER FOUND EMPTY AT: {current_timestamp}')
                        self.reset_all_variables()
        
        if adjusted_timestamp == self.end:
            if self.in_position != 0:
                logger.info(f'TIME EXIT REACHED. EXITING.')
                exit_ticker = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                if exit_ticker:
                    exit_price = exit_ticker['c']
                    exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'COVER', 'SHORT_EXIT')
                    self.signals.append(exit_trade)
                    self.reset_all_variables()
                else:
                    logger.warning(f'EXIT TICKER FOUND EMPTY AT: {current_timestamp}')
                    self.reset_all_variables()

    def run_backtest(self, uid: str):

        self.set_params_from_uid(uid)
        spot_df = self.get_spot_df(self.underlying)
        resampled_df = self.resample_df(spot_df)
        iterable = self.create_itertuples(resampled_df)

        for row in iterable:
            self.gen_signals(row)
        
        tradesheet = pd.DataFrame(self.signals)
        tradebook = self.calc.calculate_pl_in_opt_tradesheet(tradesheet)
        tradebook.to_parquet(f'C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/intradaypullback/{uid}.parquet')
        logger.info('###############################BACKTEST COMPLETE#######################################')



# if __name__ == '__main__':
#     intraday = INTRADAYPULLBACK()
#     intraday.run_backtest('INTRADAYPULLBACK_NIFTY_15_0.5_2_0_0')




        
        
        
    
