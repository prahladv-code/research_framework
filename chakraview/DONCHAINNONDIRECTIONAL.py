from chakraview.chakraview import ChakraView
import pandas as pd
import numpy as np
import datetime
from chakraview.config import sessions, lot_sizes, strike_diff
from chakraview.logger import logger
from analysis.calculate_metrics import CalculateMetrics

class DONCHAINNONDIRECTIONAL(ChakraView):
    def __init__(self):
        super().__init__()
        self.reset_all_variables()
        self.signals = []
    
    def reset_all_variables(self):
        self.in_position = 0
        self.call_entry_symbol = None
        self.put_entry_symbol = None
        self.expiry = None
    
    