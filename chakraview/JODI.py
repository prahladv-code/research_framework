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
        self.start = sessions.get('nifty').get('start')
        self.end = sessions.get('nifty').get('end')
