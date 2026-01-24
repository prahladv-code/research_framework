from chakraview.chakraview import ChakraView
import datetime

ck = ChakraView()
# df = ck.find_ticker_by_moneyness('NIFTY', 0, datetime.date(2025, 9, 2), datetime.time(9, 30), 24864.1, 50, 'PE', 5)
df = ck.find_ticker_by_delta(underlying='NIFTY', expiry_code=0, date=datetime.date(2025, 9, 2), time=datetime.time(9, 30), delta=0.5, right='CE', spot=24864.1)
print(df)