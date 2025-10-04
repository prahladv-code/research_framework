import pandas as pd
import datetime
from chakraview.chakraview import ChakraView
import time
from chakraview.config import sessions
import multiprocessing as mp
from analysis.calculate_metrics import CalculateMetrics

class PCCO(ChakraView):
    def __init__(self):
        super().__init__()
        self.sessions = sessions
        self.previous_date = None
        self.df = self.get_spot_df('nifty')
        self.metrics = CalculateMetrics()

    def reset_all_variables(self):
        self.in_position = False
        self.long_condition = False
        self.short_condition = False
        self.entry_symbol = ''
        self.entry_trade = ''
        self.exit_trade = ''
        self.position_count = 0
        self.entry_time = None
        
    
    def check_new_day(self, date):
        self.date = date
        if self.date != self.previous_date:
            self.previous_date = self.date
            return True
        elif self.date == self.previous_date:
            return False
    
    def set_signal_parameters(self, uid):
        uid_params = uid.split('_')
        self.strat = uid_params.pop(0)
        self.underlying = uid_params.pop(0)
        self.sessions = sessions.get(self.underlying)
        self.start_session = self.sessions.get('start')
        self.end_session = self.sessions.get('end')
        self.target_time_period = int(uid_params.pop(0))
        self.reentry = uid.pop(0)='True'
        self.max_pos = uid.pop(0)

    
    def create_itertuples(self):
        return self.df.itertuples(index=False)
    
    def gen_signals(self):
        iterable = self.create_itertuples()
        self.open = 0
        self.close = 0
        self.upper_bound = 0
        self.lower_bound = 0
        signals_list = []
        for self.row in iterable:
            new_day = self.check_new_day(self.row.date)
            if new_day:
                self.reset_all_variables()
            
            if self.row.time >= self.end_session:
                self.close = self.row.c
                if self.in_position:
                    print('Time Exit')
                    if self.entry_trade == 'BUY':
                        self.exit_trade = 'SELL'
                    elif self.entry_trade == 'SHORT':
                        self.exit_trade = 'COVER'
                    signals_list.append(
                        {
                            'timestamp': str(self.row.date) + str(self.row.time),
                            'symbol': self.row.symbol,
                            'price': self.row.c,
                            'trade': self.exit_trade,
                            'system action': 'TIME_EXIT'
                        }
                    )
                    self.reset_all_variables()
            elif self.row.time == self.start_session:
                self.open = self.row.o
            
            if self.start_session <= self.row.time < self.end_session:
                print("Trading Session Starts")
                if self.close != 0 and self.position_count == 0:
                    if not self.in_position:
                        if self.close > self.open:
                            self.upper_bound = self.close
                            self.lower_bound = self.open
                        elif self.open > self.close:
                            self.upper_bound = self.open
                            self.lower_bound = self.close
                        else:
                            self.upper_bound = self.close
                            self.lower_bound = self.close

                        # ENTRY CONDITION

                        if self.row.h > self.upper_bound:
                            self.log.info(f'Long Signal Triggered At {self.row.date} { self.row.time}')
                            self.entry_trade = 'BUY'
                            price = self.upper_bound
                            self.in_position = True
                            self.entry_time = datetime.datetime.combine(self.row.date, self.row.time)
                            signals_list.append(
                                {
                                    'timestamp': str(self.row.date) + str(self.row.time),
                                    'symbol': self.row.symbol,
                                    'price': price,
                                    'trade': self.entry_trade,
                                    'system action': 'LONG_ENTRY' 
                                }
                            )

                        elif self.row.l < self.lower_bound:
                            self.log.info(f'Short Signal Triggered At {self.row.date} {self.row.time}')
                            self.entry_trade = 'SHORT'
                            price = self.lower_bound
                            self.in_position = True
                            self.entry_time = datetime.datetime.combine(self.row.date, self.row.time)
                            signals_list.append(
                                {
                                    'timestamp': str(self.row.date) + '' + str(self.row.time),
                                    'symbol': self.row.symbol,
                                    'price': price,
                                    'trade': self.entry_trade,
                                    'system action': 'SHORT_ENTRY' 
                                }
                            )
                    if self.in_position and self.entry_time:
                        exit_timestamp = self.entry_time + datetime.timedelta(minutes=self.target_time_period)
                        current_timestamp = datetime.datetime.combine(self.row.date, self.row.time)
                        print(exit_timestamp)
                        # self.log.info(f'Exit TimeStamp: {exit_timestamp}')
                        if current_timestamp == exit_timestamp:
                            self.log.info(f'Exit Condition Triggered At {str(exit_timestamp)}')
                            if self.entry_trade == 'BUY':
                                self.exit_trade = 'SELL'
                                price = self.row.h
                            elif self.entry_trade == 'SHORT':
                                self.exit_trade = 'COVER'
                                price = self.row.l
                            
                            signals_list.append(
                                {
                                    'timestamp': str(self.row.date) + str(self.row.time),
                                    'symbol': self.row.symbol,
                                    'price': price,
                                    'trade': self.exit_trade,
                                    'system action': 'EXIT_CONDITION' 
                                }
                            )
                            self.in_position = False
                            self.position_count = 1
            print(f'{self.row.date} {self.row.time} Upper Bound: {self.upper_bound} Lower Bound: {self.lower_bound} New Day: {new_day}, In Position {self.in_position}')

        return signals_list
    
    
    def create_backtest(self, uid: str):
        start = time.time()
        self.set_signal_parameters(uid)
        signals = self.gen_signals()
        df = pd.DataFrame(signals)
        tradesheet = self.metrics.calculate_pl_in_tradesheet(df)
        tradesheet.to_parquet(f'C:/Users/admin/VSCode/research/research_framework/tradesheets/pcco/{uid}.parquet')
        end = time.time()
        print(f'Elapsed Time In COMPLETING raw Tradesheet Generation: {end-start}')
        print("+++++++++++++++++++++++++++++++++++++++ GENERATED UID +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            
    def gen_uids_from_minute_list(self, minute_iterations: list):
        uid_list = []
        for minute in minute_iterations:
            uid = 'PCCO_' + 'nifty_' + f'{minute}'
            uid_list.append(uid)
        print(uid_list)
        return uid_list

