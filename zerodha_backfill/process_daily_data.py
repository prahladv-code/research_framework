from zerodha_backfill.Zerodha_backfill import BackFill
from collections import defaultdict
import datetime
import os
import time
import pandas as pd


underlying_map = {
    'NIFTY 50': 'NIFTY',
    'NIFTY BANK': 'BANKNIFTY',
    'NIFTY MIDCAP SELECT (MIDCPNIFTY)': 'MIDCPNIFTY',
    'NIFTY FIN SERVICE': 'FINNIFTY'
}

ROMAN = ['I', 'II', 'III', 'IV', 'V', 'VI']


def build_expiry_rank_map(expiries: list[datetime.date]):
    """
    Maps each expiry to its series:
    nearest -> I, next -> II, next -> III ...
    """
    sorted_expiries = sorted(expiries)
    return {
        expiry: ROMAN[idx]
        for idx, expiry in enumerate(sorted_expiries)
        if idx < len(ROMAN)
    }


if __name__ == '__main__':
    start_date = datetime.datetime.today().date()
    end_date = datetime.datetime.today().date()
    today = datetime.datetime.today().date()
    backfill = BackFill()
    instruments = backfill.filter_instrument_metadata()
    kite = backfill.auth()
    today = datetime.date.today()
    combined_opt_df = []
    # 1️⃣ Collect FUT expiries per underlying
    fut_expiries_by_underlying = defaultdict(list)

    for instrument in instruments:
        if instrument.get('instrument_type') == 'FUT':
            expiry = instrument.get('expiry')
            if expiry and expiry >= today:
                fut_expiries_by_underlying[instrument['name']].append(expiry)

    # 2️⃣ Build expiry → series maps per underlying
    expiry_rank_maps = {
        underlying: build_expiry_rank_map(expiries)
        for underlying, expiries in fut_expiries_by_underlying.items()
    }

    # 3️⃣ Process instruments
    for instrument in instruments:
        token = instrument.get('instrument_token')
        symbol = instrument.get('tradingsymbol')
        expiry = instrument.get('expiry')
        underlying = instrument.get('name')
        strike = instrument.get('strike')
        instrument_type = instrument.get('instrument_type')
        segment = instrument.get('segment')

        if instrument_type == 'FUT':
            expiry_code = expiry_rank_maps[underlying].get(expiry)

            if not expiry_code:
                continue  # safety guard

            print(f'FUT TYPE INSTRUMENT FOUND: {symbol}')
            print(f'Expiry Series: {underlying}_{expiry_code}')

            ingest_folder = r"C:\Users\Prahlad\Desktop\zerodha_backfills\FUT"
            fut_data = backfill.get_historical_data(kite, token, start_date, end_date)
            print(f'Received Data: {fut_data}')
            if fut_data is not None and not fut_data.empty:
                fut_data['Time'] = fut_data['date'].dt.time
                fut_path = os.path.join(ingest_folder, f'{underlying}_{expiry_code}.csv')
                fut_data = fut_data.rename(
                    columns= {
                        'date': 'Date',
                        'open': 'Open',
                        'high': 'High',
                        'low': 'Low',
                        'close': 'Close',
                        'volume': 'Volume'
                    }
                )
                fut_data['Date'] = pd.to_datetime(fut_data['Date'])
                fut_data['Time'] = fut_data['Date'].dt.time
                fut_data['Date'] = fut_data['Date'].dt.date
                fut_data = fut_data[['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
                fut_data.to_csv(fut_path, index=False)
                print(f'Futures Data Saved To CSV Successfully.')
            else:
                print(f'Futures Data Is Empty. Skipping.')

        elif segment == 'INDICES':
            ingest_folder = r"C:\Users\Prahlad\Desktop\zerodha_backfills\SPOT"
            name = instrument.get('name')
            underlying = underlying_map.get(name)
            token = instrument.get('instrument_token')
            spot_data = backfill.get_historical_data(kite, token, start_date, end_date)
            if spot_data is not None and not spot_data.empty:
                spot_path = os.path.join(ingest_folder, f'NSE_IDX_{underlying}.csv')
                spot_data = spot_data.rename(
                    columns= {
                        'date': 'Date',
                        'open': 'Open',
                        'high': 'High',
                        'low': 'Low',
                        'close': 'Close'
                    }
                )
                spot_data['Date'] = pd.to_datetime(spot_data['Date'])
                spot_data['Time'] = spot_data['Date'].dt.time
                spot_data['Date'] = spot_data['Date'].dt.date
                spot_data = spot_data[['Date', 'Time', 'Open', 'High', 'Low', 'Close']].copy()
                spot_data.to_csv(spot_path, index=False)
                print(f'Spot Data Saved To CSV Successfully.')
            else:
                print(f'Spot Data Is Empty For Underlying: {underlying}. Skipping.')

        if instrument_type in ('CE', 'PE'):
            
            ingest_folder = r"C:\Users\Prahlad\Desktop\zerodha_backfills\OPT"
            opt_data = None
            
            if (expiry - today).days <= 120:
                opt_data = backfill.get_historical_data(kite, token, start_date, end_date)

            if opt_data is not None and not opt_data.empty:
                print(f'Found Valid Data For OPT Symbol: {symbol}')
                opt_data = opt_data.rename(
                    columns= {
                        'open': 'o',
                        'high': 'h',
                        'low': 'l',
                        'close': 'c',
                        'volume': 'v'
                    }
                )
                opt_data['date'] = pd.to_datetime(opt_data['date'])
                opt_data['time'] = opt_data['date'].dt.time
                opt_data['date'] = opt_data['date'].dt.date
                opt_data['symbol'] = symbol
                opt_data['expiry'] = expiry
                opt_data['strike'] = strike
                opt_data['right'] = instrument_type
                opt_data['underlying'] = underlying
                combined_opt_df.append(opt_data)
            else:
                print(f'Options Data Is Empty for Symbol: {symbol}')
    
    print('-----------------------Starting Concatenation Process (OPT)-------------------------')

    final_opt_df = pd.concat(combined_opt_df, ignore_index=True)
    ingest_opt_folder = r"C:\Users\Prahlad\Desktop\zerodha_backfills\OPT"
    filename_opt = start_date.strftime('%Y%m%d')
    fullpath_opt = os.path.join(ingest_opt_folder, f'OPT_DAILY_{filename_opt}.parquet')
    final_opt_df.to_parquet(fullpath_opt, index=False)



            
            

