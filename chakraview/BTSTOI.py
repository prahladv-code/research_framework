from chakraview.chakraview import ChakraView
from chakraview.config import sessions, lot_sizes, strike_diff
from chakraview.logger import logger
from analysis.calculate_metrics import CalculateMetrics
import pandas as pd
import numpy as np
import datetime

class BTSTOI(ChakraView):
    def __init__(self):
        super().__init__()
        self.reset_all_variables()
        self.signal_list = []
        self.new_day = None
        self.calc = CalculateMetrics()
    
    def reset_all_variables(self):
        self.in_position = 0
        self.entry_symbol = None
        self.expiry = None
    
    def set_params_from_uid(self, uid: str):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.timeframe = int(uid_split.pop(0))
        self.upper_ratio = float(uid_split.pop(0))
        self.lower_ratio = float(uid_split.pop(0))
        self.expiry_code = int(uid_split.pop(0))
        self.moneyness = int(uid_split.pop(0))
        self.qty = lot_sizes.get(self.underlying)
        self.strike_diff = strike_diff.get(self.underlying)
        self.start = sessions.get(self.underlying).get('start')
        self.end = sessions.get(self.underlying).get('end')
    
    def create_itertuples(self, df: pd.DataFrame):
        return df.itertuples(index=False)
    
    def check_new_day(self, date: datetime.date):
        if date != self.new_day:
            self.new_day = date
            return True
        return False
    
    def get_resampled_tick(self, date: datetime.date, time: datetime.time):
        timestamp_offset = self.timeframe - 1
        base_dt = datetime.datetime.combine(date, time)

        if time == datetime.time(15, 5):
            adjusted_timestamp = base_dt + datetime.timedelta(minutes=24)
        else:
            adjusted_timestamp = base_dt + datetime.timedelta(minutes = timestamp_offset)
        
        return adjusted_timestamp.time()
    
    def calculate_oi_parity(self, date: datetime.date, time: datetime.time, adjusted_expiry_code: int):
        tsdf = self.get_all_ticks_by_timestamp(self.underlying, adjusted_expiry_code, date, time)
        try:
            if not tsdf.empty:
                call_df = tsdf[tsdf['right'] == 'CE'].copy()
                put_df = tsdf[tsdf['right'] == 'PE'].copy()
                call_oi = call_df['oi'].sum()
                put_oi = put_df['oi'].sum()
                oi_parity = put_oi/call_oi
                return oi_parity
            else:
                logger.warning(f'NO TIMESTAMP FOUND FOR {date} {time}')
                return None
        except Exception as e:
            logger.error(f'Error In Fetching Timestamp DataFrame: {e}')
        
    
    def gen_signals(self, row):
        self.adjusted_expiry_code = 0
        current_timestamp = f'{row.date} {row.time}'
        new_day = self.check_new_day(row.date)
        if row.date == self.expiry:
            if self.expiry_code == 0:
                self.adjusted_expiry_code = self.expiry_code + 1
        else:
            self.adjusted_expiry_code = self.expiry_code
        
        if row.time == datetime.time(15, 27, 0):
            oi_parity = self.calculate_oi_parity(row.date, row.time, self.adjusted_expiry_code)
            if oi_parity is not None:
                if oi_parity >= self.upper_ratio:
                    if self.in_position == 0:
                        logger.info(f'CALL LONG SIGNAL FOUND AT {current_timestamp}')
                        entry_tick = self.find_ticker_by_moneyness(self.underlying, self.adjusted_expiry_code, row.date, row.time, row.c, self.strike_diff, 'CE', self.moneyness)
                        if entry_tick:
                            self.entry_symbol = entry_tick['symbol']
                            self.expiry = entry_tick['expiry'].date()
                            entry_price = entry_tick['c']
                            entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'BUY', 'LONG_ENTRY')
                            self.signal_list.append(entry_trade)
                            self.in_position = 1
                        else:
                            logger.warning(f'CALL LONG TICK FOUND EMPTY AT {current_timestamp}')
                            self.reset_all_variables()
                
                elif oi_parity <= self.lower_ratio:
                    if self.in_position == 0:
                        logger.info(f'PUT LONG SIGNAL FOUND AT {current_timestamp}')
                        entry_tick = self.find_ticker_by_moneyness(self.underlying, self.adjusted_expiry_code, row.date, row.time, row.c, self.strike_diff, 'PE', self.moneyness)
                        if entry_tick:
                            self.entry_symbol = entry_tick['symbol']
                            self.expiry = entry_tick['expiry'].date()
                            entry_price = entry_tick['c']
                            entry_trade = self.place_trade(current_timestamp, self.entry_symbol, entry_price, self.qty, self.qty * entry_price, 'BUY', 'LONG_ENTRY')
                            self.signal_list.append(entry_trade)
                            self.in_position = -1
                        else:
                            logger.warning(f'PUT LONG TICK FOUND EMPTY AT {current_timestamp}')
                            self.reset_all_variables()

        if new_day:                
            if row.time == datetime.time(9, 15, 0):
                if self.in_position == -1:
                    logger.info(f'PUT LONG EXIT SIGNAL FOUND AT {current_timestamp}')
                    exit_tick = self.get_tick(self.entry_symbol, row.date, row.time)
                    if exit_tick:
                        exit_price = exit_tick['c']
                        exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'SELL', 'LONG_EXIT')
                        self.signal_list.append(exit_trade)
                        self.reset_all_variables()
                    else:
                        logger.warning(f'PUT LONG EXIT TICK FOUND EMPTY AT: {current_timestamp}')
                        self.reset_all_variables()

                elif self.in_position == 1:
                    logger.info(f'CALL LONG EXIT SIGNAL FOUND AT {current_timestamp}')
                    exit_tick = self.get_tick(self.entry_symbol, row.date, row.time)
                    if exit_tick:
                        exit_price = exit_tick['c']
                        exit_trade = self.place_trade(current_timestamp, self.entry_symbol, exit_price, self.qty, self.qty * exit_price, 'SELL', 'LONG_EXIT')
                        self.signal_list.append(exit_trade)
                        self.reset_all_variables()
                    else:
                        logger.warning(f'CALL LONG EXIT TICK FOUND EMPTY AT: {current_timestamp}')
                        self.reset_all_variables()


    def run_backtest(self, uid: str):
        self.set_params_from_uid(uid)
        spot_df = self.get_spot_df(self.underlying)
        spot_df = spot_df[(spot_df['time'] >= self.start) & (spot_df['time'] <= self.end)].reset_index(drop=True).copy()
        spot_itertuples = self.create_itertuples(spot_df)
        for row in spot_itertuples:
            self.gen_signals(row)
        tradebook = pd.DataFrame(self.signal_list)
        tradesheet = self.calc.calculate_pl_in_opt_tradesheet(tradebook)
        tradesheet.to_parquet(f'C:/Users/Admin/Desktop/research_framework/research_framework/tradesheets/btst/{uid}.parquet')
        logger.info("#####################################BACKTEST COMPLETE#######################################")


            
            