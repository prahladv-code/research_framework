import pandas as pd
from analysis import calculate_metrics
from chakraview import ChakraView
import numpy as np
import datetime
import time
import logging
from config import sessions



class JODI(ChakraView):
    def __init__(self):
        super().__init__()
        self.start = sessions.get('nifty').get('start')
        self.end = sessions.get('nifty').get('end')
        self.previous_date = None
    
    def reset_all_variables(self):
        pass

    def check_newday(self, date):
        self.date = date
        if self.date != self.previous_date:
            return True
        elif self.date == self.previous_date:
            return False

    def set_signal_params(self, uid: str):
        uid_split = uid.split('_')
        self.strat = uid_split.pop(0)
        self.underlying = uid_split.pop(0)
        self.ma_period = uid_split.pop(0)
    
    