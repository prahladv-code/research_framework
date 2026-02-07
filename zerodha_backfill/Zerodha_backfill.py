import pandas as pd
import datetime
from kiteconnect import KiteConnect
import json
import time
from direct_redis import DirectRedis
r = DirectRedis(host='192.168.1.28', port='6379', db=0)

class BackFill:

    def __init__(self):
        """Parent Class For Daily Historical BackFill"""

    def auth(self):
        access_token = r.get('kite_access_token')
        print(f'Access Token Debug: {access_token}')
        kite = KiteConnect(api_key='nj3nfk5lxbm4fp4j')
        kite.set_access_token(access_token)
        return kite

    def get_instrument_metadata(self, exchange_list: list):
        kite = self.auth()
        meta_list = []
        for exchange in exchange_list:
            instruments = kite.instruments(exchange)
            meta_list.extend(instruments)

        return meta_list

    def filter_instrument_metadata(self):
        instruments = self.get_instrument_metadata(['NSE', 'NFO', 'MCX'])
        final_dict = []
        today = datetime.datetime.today().date()
        accepted_names = ['NIFTY', 'BANKNIFTY', 'MIDCPNIFTY', 'FINNIFTY', 'GOLD', 'CRUDEOIL', 'NIFTY 50', 'NIFTY BANK', 'NIFTY MIDCAP SELECT (MIDCPNIFTY)', 'NIFTY FIN SERVICE']
        for instrument in instruments:
            symbol = instrument.get('tradingsymbol')
            instrument_token = instrument.get('instrument_token')
            name = instrument.get('name')
            expiry = instrument.get('expiry')
            segment = instrument.get('segment')

            if name in accepted_names:
                final_dict.append(instrument)
            
        print(f'Length Of Total Symbols Being Pushed: {len(final_dict)}')
        return final_dict

    def json_serializer(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    def get_historical_data(self, kite: KiteConnect, token_id: int, start_date: datetime.date, end_date: datetime.date):
        start_dt_str = start_date.strftime('%Y-%m-%d')
        end_dt_str = end_date.strftime('%Y-%m-%d')
        data = kite.historical_data(instrument_token=token_id, from_date=start_dt_str, to_date=end_dt_str, interval='minute', oi=True)
        time.sleep(0.25)
        df = pd.DataFrame(data)
        return df


