from chakraview.chakraview import ChakraView
import pandas as pd
import numpy as np
import datetime
from chakraview.config import sessions, lot_sizes, strike_diff
from chakraview.logger import logger
from analysis.calculate_metrics import CalculateMetrics


class DONCHAIN(ChakraView):

    def __init__(self):
        super().__init__()
        self.reset_all_variables()
        self.calc = CalculateMetrics()
        self.signals = []
    
    def reset_all_variables(self):
        self.in_position = 0
        self.entry_symbol = None
        self.expiry = None
        
        
    # 'DONCHAINBTST_NIFTY_25_20_1_0_0'
    def set_params_from_uid(self, uid: str):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.timeframe = int(uid_split.pop(0))
        self.indicator_period = int(uid_split.pop(0))
        self.offset = int(uid_split.pop(0))
        self.moneyness = int(uid_split.pop(0))
        self.expiry_code = int(uid_split.pop(0))
        self.start = sessions.get(self.underlying).get('start')
        self.end = sessions.get(self.underlying).get('end')
        self.qty = lot_sizes.get(self.underlying)
        self.strike_diff = strike_diff.get(self.underlying)
    
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
    
    def calculate_donchain_channel(self, df: pd.DataFrame) -> pd.DataFrame:

        """Calculates Donchain Channel Indicator For Any Time Series DataFrame."""
        df['upper_channel'] = df['h'].rolling(self.indicator_period).max()
        df['upper_channel'] = df['upper_channel'].shift(self.offset)
        df['lower_channel'] = df['l'].rolling(self.indicator_period).min()
        df['lower_channel'] = df['lower_channel'].shift(self.offset)
        
        return df
    
    def create_itertuples(self, db: pd.DataFrame):
        return db.itertuples(index=False)
    
    def gen_signals(self, row):

        current_timestamp = f'{row.date} {row.time}'
        adjusted_timestamp = self.get_resampled_tick(row.date, row.time)

        #ENTRY
        if adjusted_timestamp == self.end:
            if self.in_position == 0:
                if row.c > row.upper_channel:
                    entry_tick = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'CE', self.moneyness)
                    if entry_tick:
                        self.expiry = entry_tick.get('expiry').date()

                        if row.date == self.expiry:
                            logger.info(f'ENTRY SIGNAL FOUND ON EXPIRY DAY. TAKING NEXT EXPIRY.')
                            next_entry_tick = self.find_ticker_by_moneyness(self.underlying, self.expiry_code + 1, row.date, adjusted_timestamp, row.c, self.strike_diff, 'CE', self.moneyness)

                            if not next_entry_tick:
                                logger.warning(f"ENTRY TICK FOUND EMPTY AT: {current_timestamp}")
                                self.reset_all_variables()
                                return
                            
                            entry_tick = next_entry_tick
                        
                        logger.info(f'CALL LONG SIGNAL FOUND AT: {current_timestamp}')
                        entry_price = entry_tick.get('c')
                        self.expiry = entry_tick.get('expiry').date()
                        self.entry_symbol = entry_tick.get('symbol')
                        entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'BUY', 'LONG_ENTRY')
                        self.signals.append(entry_trade)
                        self.in_position = 1
                    else:
                        logger.warning(f'ENTRY TICK FOUND EMPTY AT: {current_timestamp}')
                        self.reset_all_variables()

                elif row.c < row.lower_channel:
                    entry_tick = self.find_ticker_by_moneyness(self.underlying, self.expiry_code, row.date, adjusted_timestamp, row.c, self.strike_diff, 'PE', self.moneyness)
                    if entry_tick:
                        self.expiry = entry_tick.get('expiry').date()

                        if row.date == self.expiry:
                            logger.info(f'ENTRY SIGNAL FOUND ON EXPIRY DAY. TAKING NEXT EXPIRY.')
                            next_entry_tick = self.find_ticker_by_moneyness(self.underlying, self.expiry_code + 1, row.date, adjusted_timestamp, row.c, self.strike_diff, 'PE', self.moneyness)

                            if not next_entry_tick:
                                logger.warning(f"ENTRY TICK FOUND EMPTY AT: {current_timestamp}")
                                self.reset_all_variables()
                                return
                            
                            entry_tick = next_entry_tick

                        logger.info(f'PUT LONG SIGNAL FOUND AT: {current_timestamp}')
                        entry_price = entry_tick.get('c')
                        self.expiry = entry_tick.get('expiry').date()
                        self.entry_symbol = entry_tick.get('symbol')
                        entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'BUY', 'LONG_ENTRY')
                        self.signals.append(entry_trade)
                        self.in_position = -1
                    else:
                        logger.warning(f'ENTRY TICK FOUND EMPTY AT: {current_timestamp}')
                        self.reset_all_variables()
        
        #EXIT
        if row.time == self.start:

            if self.in_position == 1:
                exit_tick = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                if exit_tick:
                    logger.info(f'CALL LONG EXIT SIGNAL FOUND AT: {current_timestamp}')
                    exit_price = exit_tick.get('c')
                    exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'SELL', 'LONG_EXIT')
                    self.signals.append(exit_trade)
                    self.reset_all_variables()
                else:
                    logger.warning(f'EXIT TICK FOUND EMPTY AT: {current_timestamp}')
                    self.reset_all_variables()
            
            elif self.in_position == -1:
                exit_tick = self.get_tick(self.entry_symbol, row.date, adjusted_timestamp)
                if exit_tick:
                    logger.info(f'PUT LONG EXIT SIGNAL FOUND AT: {current_timestamp}')
                    exit_price = exit_tick.get('c')
                    exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'SELL', 'LONG_EXIT')
                    self.signals.append(exit_trade)
                    self.reset_all_variables()
                else:
                    logger.warning(f'EXIT TICK FOUND EMPTY AT: {current_timestamp}')
                    self.reset_all_variables()

    def run_backtest(self, uid: str):
        self.set_params_from_uid(uid)
        spot_df = self.get_spot_df(self.underlying)
        resampled_df = self.resample_df(spot_df)
        donchain_df = self.calculate_donchain_channel(resampled_df)
        iterable = self.create_itertuples(donchain_df)
        for row in iterable:
            self.gen_signals(row)
        
        tradesheet = pd.DataFrame(self.signals)
        tradebook = self.calc.calculate_pl_in_opt_tradesheet(tradesheet)
        tradebook.to_parquet(f"C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/donchainbtst/{uid}.parquet")
        logger.info("##########################BACKTEST COMPLETE###################################")


# if __name__ == '__main__':
#     dc = DONCHAIN()
#     dc.run_backtest('DONCHAINBTST_NIFTY_25_30_1_0_0')
    
