from chakraview.VWAP import VWAP
import datetime
import pandas as pd
from chakraview.chakraview import ChakraView

vwap = VWAP()
ck = ChakraView()
# df = vwap.get_all_ticks_by_timestamp('NIFTY', 0, datetime.date(2025, 9, 1), datetime.time(9, 15, 0), 24432.7)
df = vwap.find_ticker_by_moneyness(underlying='NIFTY', expiry_code=1, date=datetime.date(2025, 9, 2), time=datetime.time(9, 16), underlying_price=24432.7, strike_difference=50, right='CE', moneyness= 0)
symbol = df.get('symbol')
signals_df = vwap.get_all_ticks_by_symbol(symbol)
print(signals_df)

# print(ck.get_all_ticks_by_timestamp('NIFTY', 0, datetime.date(2025, 9, 2), datetime.time(9, 16)))