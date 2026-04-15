from chakraview.chakraview import ChakraView
import datetime
import pandas as pd
from chakraview.config import sessions, continuous_codes, lot_sizes, strike_diff
import multiprocessing as mp
from analysis.calculate_metrics import CalculateMetrics
import numpy as np
import time
from chakraview.logger import logger

class PRICEMA(ChakraView):
    def __init__(self):
        super().__init__()
        self.previous_date = None
        self.metrics = CalculateMetrics()
        self.continuous_codes = continuous_codes
        self.signals_list = []
        self.reset_all_variables()
        
    def reset_all_variables(self):
        self.entry_price = np.nan
        self.exit_price = np.nan
        self.short_condition = False
        self.long_condition = False
        self.entry_trade = ''
        self.entry_time = None
        self.entry_price = None
        self.position_count = 0
        self.entry_symbol = None
        self.expiry = None
        self.in_position = 0
        self.entry_symbol = None
        
    
    def check_new_day(self, date):
        self.date = date
        if self.date != self.previous_date:
            self.previous_date = self.date
            return True
        elif self.date == self.previous_date:
            return False


    def set_signal_parameters(self, uid):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.expiry_code = int(uid_split.pop(0))
        self.fut_series = self.continuous_codes.get(self.expiry_code)
        self.ma_period = int(uid_split.pop(0))
        self.timeframe = uid_split.pop(0)
        self.atr_multiplier = float(uid_split.pop(0))
        self.qty = lot_sizes.get(self.underlying)
        self.start_time = sessions.get(self.underlying).get('start')
        self.end_time = sessions.get(self.underlying).get('end')
        self.strike_diff = strike_diff.get(self.underlying)
    
    def get_actual_event_timestamp(self, timestamp: datetime):
        timeframe = int(self.timeframe)
        offset = timeframe - 1
        actual_timestamp = timestamp + datetime.timedelta(minutes=offset)
        return actual_timestamp
    
    def create_itertupes(self, db):
        return db.itertuples(index=False)
    
    def generate_uid_from_parameters(self, underlying, expiry_code, ma_period, timeframe, reentry):
        return f'PRICEMA_{underlying}_{expiry_code}_{ma_period}_{timeframe}_{reentry}'


    def generate_resampled_timestamps(self):
        int_timeframe = int(self.timeframe)
        print(f'TimeFrame Debug: {self.timeframe}')
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
    
    def calculate_pricemabands(self, db):

        # compute ATR
        db['prev_close'] = db['c'].shift(1)
        db['tr'] = np.maximum.reduce([
            db['h'] - db['l'],
            (db['h'] - db['prev_close']).abs(),
            (db['l'] - db['prev_close']).abs()
        ])
        db['atr'] = db['tr'].rolling(self.ma_period).mean()
        db['ma'] = db['c'].rolling(self.ma_period).mean()
        db['upperband'] = db['ma'] + self.atr_multiplier * db['atr']
        db['lowerband'] = db['ma'] - self.atr_multiplier * db['atr']
        return db
    

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
        int_timeframe = int(self.timeframe)
        timestamp = datetime.datetime.combine(date, time)
        timestamp_offset = int_timeframe - 1
        adjusted_timestamp = timestamp + datetime.timedelta(minutes=timestamp_offset)
        adjusted_time = adjusted_timestamp.time()
        return adjusted_time

    def gen_signals(self, row):
        new_day = self.check_new_day(row.timestamp.date())
        logger.debug(f'EXPIRY DEBUG: {self.expiry}')
        #ENTRY
        if row.c > row.upperband:
            adjusted_timestamp = self.get_resampled_timestamp(row.date, row.time)
            if self.in_position == -1:
                exit_ticker = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                if exit_ticker:
                    logger.info(f'SHORT EXIT TICK FOUND AT: {row.date} {row.time}')
                    self.exit_price = exit_ticker.get('c')
                    self.exit_trade = 'COVER'
                    
                    self.signals_list.append(
                                {
                                    'timestamp': row.timestamp,
                                    'symbol': self.entry_symbol,
                                    'price': self.exit_price,
                                    'qty': self.qty,
                                    'cv': self.qty*self.exit_price if self.exit_price else 0,
                                    'trade': self.exit_trade,
                                    'system action': 'SHORT_EXIT' 
                                }
                    )
                    self.reset_all_variables()
                else:
                    logger.warning(f'SHORT EXIT TICK FOUND EMPTY AT {row.date} {row.time}')
                    self.reset_all_variables()
                    
                entry_ticker = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'PE', 0)
                if entry_ticker:
                    self.in_position = 1
                    logger.info(f"LONG ENTRY SIGNAL FOUND AT {row.date} {row.time}")
                    self.entry_symbol = entry_ticker.get('symbol')
                    self.expiry = entry_ticker.get('expiry').date()
                    entry_price = entry_ticker.get('c')
                    entry_trade = 'SHORT'
                    self.signals_list.append(
                                {
                                    'timestamp': row.timestamp,
                                    'symbol': self.entry_symbol,
                                    'price': entry_price,
                                    'qty': self.qty,
                                    'cv': self.qty*entry_price if entry_price else 0,
                                    'trade': entry_trade,
                                    'system action': 'SHORT_ENTRY' 
                                }
                    )
                else:
                    logger.warning(f'LONG ENTRY TICK FOUND EMPTY AT: {row.date} {row.time}')
                    self.reset_all_variables()
                    
            elif self.in_position == 0:
                entry_ticker = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'PE', 0)
                if entry_ticker:
                    entry_price = entry_ticker.get('c')
                    entry_trade = 'SHORT'
                    self.in_position = 1
                    self.entry_symbol = entry_ticker.get('symbol')
                    self.expiry = entry_ticker.get('expiry').date()
                    logger.info(f'LONG ENTRY SIGNAL FOUND AT {row.date} {row.time}')
                    self.signals_list.append(
                                {
                                    'timestamp': row.timestamp,
                                    'symbol': self.entry_symbol,
                                    'price': entry_price,
                                    'qty': self.qty,
                                    'cv': self.qty*entry_price if self.entry_price else 0,
                                    'trade': self.entry_trade,
                                    'system action': 'SHORT_ENTRY' 
                                }
                    )
                else:
                    logger.warning(f'LONG ENTRY TICKER FOUND EMPTY AT {row.date} {row.time}')
                    self.reset_all_variables()

        if row.c < row.lowerband:
            adjusted_timestamp = self.get_resampled_timestamp(row.date, row.time)
            if self.in_position == 1:
                exit_ticker = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                if exit_ticker:
                    logger.info(f'SHORT EXIT TICK FOUND AT: {row.date} {row.time}')
                    self.exit_price = exit_ticker.get('c')
                    self.exit_trade = 'COVER'
                    
                    self.signals_list.append(
                                {
                                    'timestamp': row.timestamp,
                                    'symbol': self.entry_symbol,
                                    'price': self.exit_price,
                                    'qty': self.qty,
                                    'cv': self.qty*self.exit_price if self.exit_price else 0,
                                    'trade': self.exit_trade,
                                    'system action': 'SHORT_EXIT' 
                                }
                    )
                    self.reset_all_variables()
                else:
                    logger.warning(f'SHORT EXIT TICK FOUND EMPTY AT {row.date} {row.time}')
                    self.reset_all_variables()
                    
                entry_ticker = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'CE', 0)
                if entry_ticker:
                    self.in_position = -1
                    logger.info(f"SHORT ENTRY SIGNAL FOUND AT {row.date} {row.time}")
                    self.entry_symbol = entry_ticker.get('symbol')
                    self.expiry = entry_ticker.get('expiry').date()
                    entry_price = entry_ticker.get('c')
                    entry_trade = 'SHORT'
                    self.signals_list.append(
                                {
                                    'timestamp': row.timestamp,
                                    'symbol': self.entry_symbol,
                                    'price': entry_price,
                                    'qty': self.qty,
                                    'cv': self.qty*entry_price if entry_price else 0,
                                    'trade': entry_trade,
                                    'system action': 'SHORT_ENTRY' 
                                }
                    )
                else:
                    logger.warning(f'SHORT ENTRY TICK FOUND EMPTY AT: {row.date} {row.time}')
                    self.reset_all_variables()

            elif self.in_position == 0:
                entry_ticker = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'CE', 0)
                if entry_ticker:
                    entry_price = entry_ticker.get('c')
                    entry_trade = 'SHORT'
                    self.in_position = -1
                    self.entry_symbol = entry_ticker.get('symbol')
                    self.expiry = entry_ticker.get('expiry').date()
                    logger.info(f'SHORT ENTRY SIGNAL FOUND AT {row.date} {row.time}')
                    self.signals_list.append(
                                {
                                    'timestamp': row.timestamp,
                                    'symbol': self.entry_symbol,
                                    'price': entry_price,
                                    'qty': self.qty,
                                    'cv': self.qty*entry_price if self.entry_price else 0,
                                    'trade': self.entry_trade,
                                    'system action': 'SHORT_ENTRY' 
                                }
                    )
                else:
                    logger.warning(f'SHORT ENTRY TICKER FOUND EMPTY AT {row.date} {row.time}')
                    self.reset_all_variables()
                    
        if row.date == self.expiry:
            adjusted_timestamp = self.get_resampled_timestamp(row.date, row.time)
            logger.debug(f'Adjusted Timestamp Acquired: {adjusted_timestamp}')
            if adjusted_timestamp == self.end_time:
                if self.in_position == -1:
                    logger.debug("SHORT POSITION EXPIRED #####################################")
                    exit_ticker = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                    if exit_ticker:
                        logger.info(f'EXPIRY EXIT TICK FOUND AT: {row.date} {row.time}')
                        self.exit_price = exit_ticker.get('c')
                        self.exit_trade = 'COVER'
                        
                        self.signals_list.append(
                                    {
                                        'timestamp': row.timestamp,
                                        'symbol': self.entry_symbol,
                                        'price': self.exit_price,
                                        'qty': self.qty,
                                        'cv': self.qty*self.exit_price if self.exit_price else 0,
                                        'trade': self.exit_trade,
                                        'system action': 'SHORT_EXIT' 
                                    }
                        )
                        self.reset_all_variables()
                    else:
                        logger.warning(f'EXPIRY EXIT TICK FOUND EMPTY AT {row.date} {row.time}')
                        self.reset_all_variables()
                    
                    entry_ticker = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'CE', 0)
                    if entry_ticker:
                        entry_price = entry_ticker.get('c')
                        entry_trade = 'SHORT'
                        self.in_position = -1
                        self.entry_symbol = entry_ticker.get('symbol')
                        self.expiry = entry_ticker.get('expiry').date()
                        logger.info(f'SHORT ENTRY SIGNAL FOUND AT {row.date} {row.time}')
                        self.signals_list.append(
                                    {
                                        'timestamp': row.timestamp,
                                        'symbol': self.entry_symbol,
                                        'price': entry_price,
                                        'qty': self.qty,
                                        'cv': self.qty*entry_price if self.entry_price else 0,
                                        'trade': self.entry_trade,
                                        'system action': 'SHORT_ENTRY' 
                                    }
                        )
                    else:
                        logger.warning(f'SHORT ENTRY TICKER FOUND EMPTY AT {row.date} {row.time}')
                        self.reset_all_variables()

                elif self.in_position == 1:
                    logger.debug("SHORT POSITION EXPIRED #####################################")
                    exit_ticker = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                    if exit_ticker:
                        logger.info(f'EXPIRY EXIT TICK FOUND AT: {row.date} {row.time}')
                        self.exit_price = exit_ticker.get('c')
                        self.exit_trade = 'COVER'
                        
                        self.signals_list.append(
                                    {
                                        'timestamp': row.timestamp,
                                        'symbol': self.entry_symbol,
                                        'price': self.exit_price,
                                        'qty': self.qty,
                                        'cv': self.qty*self.exit_price if self.exit_price else 0,
                                        'trade': self.exit_trade,
                                        'system action': 'SHORT_EXIT' 
                                    }
                        )
                        self.reset_all_variables()
                    else:
                        logger.warning(f'EXPIRY EXIT TICK FOUND EMPTY AT {row.date} {row.time}')
                        self.reset_all_variables()
                    
                    entry_ticker = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'PE', 0)
                    if entry_ticker:
                        entry_price = entry_ticker.get('c')
                        entry_trade = 'SHORT'
                        self.in_position = 1
                        self.entry_symbol = entry_ticker.get('symbol')
                        self.expiry = entry_ticker.get('expiry').date()
                        logger.info(f'LONG ENTRY SIGNAL FOUND AT {row.date} {row.time}')
                        self.signals_list.append(
                                    {
                                        'timestamp': row.timestamp,
                                        'symbol': self.entry_symbol,
                                        'price': entry_price,
                                        'qty': self.qty,
                                        'cv': self.qty*entry_price if self.entry_price else 0,
                                        'trade': self.entry_trade,
                                        'system action': 'SHORT_ENTRY' 
                                    }
                        )
                    else:
                        logger.warning(f'LONG ENTRY TICKER FOUND EMPTY AT {row.date} {row.time}')
                        self.reset_all_variables()
            
        return self.signals_list

    def run_backtest(self, uid: str):
        start = time.time()
        self.set_signal_parameters(uid)
        commodities_underlying = None
        if self.underlying in ['GOLD', 'CRUDEOIL']:
            commodities_underlying = self.underlying + '_' + self.fut_series

        if commodities_underlying is not None:
            self.db = self.get_spot_df(commodities_underlying)
        else:
            self.db = self.get_spot_df(self.underlying)
            
        df = self.db.copy()
        resampled_df = self.resample_df(df)
        resampled_df = resampled_df.sort_values(by='timestamp').reset_index(drop = True)
        indicator_df = self.calculate_pricemabands(resampled_df)
        indicator_df = indicator_df[(indicator_df['time'] >= datetime.time(9, 15)) & (indicator_df['time'] < datetime.time(15, 30, 0))]
        iterable = self.create_itertupes(indicator_df)
        for row in iterable:
            self.gen_signals(row)
        df = pd.DataFrame(self.signals_list)
        tradesheet = self.metrics.calculate_pl_in_opt_tradesheet(df)
        tradesheet.to_parquet(f'C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/pricemabands/{uid}.parquet')
        end = time.time()
        print(f'Elapsed Time In COMPLETING raw Tradesheet Generation: {end-start}')
        print("+++++++++++++++++++++++++++++++++++++++ GENERATED UID +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

