import pandas as pd
import numpy as np
import duckdb
import datetime
import time as t
import logging
from scipy.stats import norm

class ChakraView:
    def __init__(self):
        self.daily_tb = duckdb.connect(r"C:\Users\admin\Desktop\DuckDB\nifty_daily.ddb", read_only=True)
        logging.basicConfig(filename=r'./ck_logger.log',
                                       level = logging.INFO,
                                       format="%(asctime)s [%(levelname)s] %(message)s")
        self.log = logging.getLogger(self.__class__.__name__)
    
    def get_spot_df(self, spot_name):
        df = self.daily_tb.execute(f"SELECT * FROM {spot_name}").fetch_df()
        df['date'] = pd.to_datetime(df['date']).dt.date
        df = df.rename(columns={    
                                    'open': 'o',
                                    'close': 'c',
                                    'high': 'h',
                                    'low': 'l',
                                    'volume': 'v',
                                    'oi': 'oi'  # optional, just keep as is
                                })
        return df

    def get_df(self, dfname):
        df = self.daily_tb.execute(f"SELECT * FROM {dfname}").fetch_df()
        return df

    def get_tick(self, symbol: str, date: datetime.date, time: datetime.time):
        start = t.time()
        date_str = date.strftime('%Y-%m-%d')
        time_str = time.strftime('%H:%M:%S')
        self.tick_query = f"""
        SELECT * FROM "{date_str}" WHERE time = '{time_str}' AND symbol = '{symbol}'
        """
        df = self.daily_tb.execute(self.tick_query).fetchdf()
        df_renamed = df.rename(columns={'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c', 'volume': 'v'})
        df_filtered = df_renamed[['o', 'h', 'l', 'c', 'v', 'oi']].copy()
        
        if df_filtered.empty:
            return {}
        
        tick_dict = df_filtered.to_dict(orient = 'records')
        end = t.time()
        print(f'Elapsed Time In Getting Tick: {end-start}')
        return tick_dict[0]
    
    def get_all_ticks_by_timestamp(self, date: datetime.date, time: datetime.time):
        start = t.time()
        date_str = date.strftime('%Y-%m-%d')
        time_str = time.strftime('%H:%M:%S')
        self.all_tick_query = f"""
        SELECT * FROM "{date_str}" WHERE time = '{time_str}'
        """
        df = self.daily_tb.execute(self.all_tick_query).fetchdf()
        df_renamed = df.rename(columns={'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c', 'volume': 'v'})
        df_filtered = df_renamed[['symbol', 'o', 'h', 'l', 'c', 'v', 'oi', 'strike', 'right']].copy()
        end = t.time()
        print(f'Elapsed Time In Getting All Ticks: {end-start}')
        return df_filtered
    
    def get_strike_by_moneyness(self, underlying_price, strike_difference, moneyness, right):
        """Calculates Nearest Strike According To Moneyness And Returns An Integer Value."""
        strike = 0
        atm_strike = round(underlying_price/strike_difference) * strike_difference
        if right.upper() == 'CE':
            strike = int(atm_strike + (moneyness*strike_difference))
            return strike
        elif right.upper() == 'PE':
            strike = int(atm_strike - (moneyness*strike_difference))
            return strike
        else:
            raise ValueError("Please Check The Right Input. Valid Values are [CE, PE]")
        

    # def get_all_ticks_by_symbol(self):

    def find_ticker_by_moneyness(self, date, time, underlying_price, strike_difference, right, moneyness):
        start = t.time()
        all_timestamp_df = self.get_all_ticks_by_timestamp(date, time)
        strike = self.get_strike_by_moneyness(underlying_price, strike_difference, moneyness, right)
        moneyness_df = all_timestamp_df[(all_timestamp_df['right'] == right.upper()) & (all_timestamp_df['strike'] == strike)]
        if moneyness_df.empty:
            self.log.error(f'No Data For TimeStamp: {date} {time}')
            return {}
        moneyness_dict = moneyness_df.to_dict(orient='records')
        end=t.time()
        print(f'Elapsed Time In Getting Moneyness Details: {end-start}')
        return moneyness_dict[0]

    def find_ticker_by_premium(self, date, time, underlying_price, right, premium_val, atm_filter=False):
        start = t.time()

        all_timestamp_df = self.get_all_ticks_by_timestamp(date, time)
        option_chain = all_timestamp_df[all_timestamp_df['right'] == right.upper()].copy()

        if option_chain.empty:
            self.log.error(f'No Data For TimeStamp: {date} {time}')
            return {}
        
        option_chain['premium_diff'] = abs(option_chain['c'] - premium_val)
        min_row = option_chain.loc[option_chain['premium_diff'].idxmin()]
        min_row_dict = min_row.to_dict()
        end = t.time()
        print(f'Elapsed Time In Getting Premium Details: {end-start}')
        return min_row_dict
    
    def find_ticker_by_delta(self, date, time, delta, right, spot):
        tsdf = self.get_all_ticks_by_timestamp(date, time)

        # ---------------------------
        # Black-Scholes Pricing
        # ---------------------------
        def bs_price(opt_type, S, K, r, sigma, T):
            if T <= 0 or sigma <= 0:
                return np.nan
            
            d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
            d2 = d1 - sigma*np.sqrt(T)

            if opt_type == "CE":
                return S*norm.cdf(d1) - K*np.exp(-r*T)*norm.cdf(d2)
            else:
                return K*np.exp(-r*T)*norm.cdf(-d2) - S*norm.cdf(-d1)


        # ---------------------------
        # Implied Volatility Solver
        # ---------------------------
        def solve_iv(opt_type, S, K, r, T, market_price, tol=1e-6, max_iter=100):
            sigma = 0.20  # Start guess

            for _ in range(max_iter):

                price = bs_price(opt_type, S, K, r, sigma, T)
                if np.isnan(price):
                    return np.nan

                # Vega
                d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
                vega = S * norm.pdf(d1) * np.sqrt(T)
                if vega < 1e-8:
                    return np.nan

                diff = price - market_price
                sigma_new = sigma - diff / vega

                if abs(sigma_new - sigma) < tol:
                    return max(sigma_new, 1e-6)

                sigma = sigma_new

            return np.nan


        # ---------------------------
        # Main Function
        # ---------------------------
        def compute_iv_delta(tsdf: pd.DataFrame, date, spot, r: float = 0.07):
            # --------------------------------------------------------
            # 1. Extract Spot Price (use "NIFTY" row)
            # --------------------------------------------------------
            spot = spot

            # --------------------------------------------------------
            # 2. Parse Expiry from symbols
            # --------------------------------------------------------
            def extract_expiry(sym: str):
                # Option symbols look like: NIFTY02JAN2521600PE
                # Extract substring at positions 5 to 12 → "02JAN25"
                if len(sym) >= 12 and sym.startswith("NIFTY"):
                    part = sym[5:12]  # DDMMMYY
                    try:
                        return datetime.datetime.strptime(part, "%d%b%y")
                    except:
                        return pd.NaT
                return pd.NaT

            tsdf["expiry"] = tsdf["symbol"].apply(extract_expiry)

            # Convert to pandas datetime explicitly
            tsdf["expiry"] = pd.to_datetime(tsdf["expiry"], errors="coerce")

            # --------------------------------------------------------
            # 3. Compute time to maturity
            # --------------------------------------------------------
            today = pd.to_datetime(date)
            tsdf["ttm"] = (tsdf["expiry"] - today).dt.days / 365.0

            # --------------------------------------------------------
            # 4. Compute IV
            # --------------------------------------------------------
            ivs = []

            for _, row in tsdf.iterrows():

                if row["right"] not in ("CE", "PE"):
                    ivs.append(np.nan)
                    continue

                iv = solve_iv(
                    opt_type=row["right"],
                    S=spot,
                    K=row["strike"],
                    r=r,
                    T=row["ttm"],
                    market_price=row["c"]
                )
                ivs.append(iv)

            tsdf["iv"] = ivs

            # --------------------------------------------------------
            # 5. Compute Delta
            # --------------------------------------------------------
            deltas = []

            for _, row in tsdf.iterrows():

                if row["right"] not in ("CE", "PE"):
                    deltas.append(np.nan)
                    continue

                S = spot
                K = row["strike"]
                sigma = row["iv"]
                T = row["ttm"]

                if pd.isna(sigma) or sigma <= 0 or T <= 0:
                    deltas.append(np.nan)
                    continue

                d1 = (np.log(S/K) + (r + 0.5*sigma*sigma)*T) / (sigma*np.sqrt(T))

                if row["right"] == "CE":
                    delta = norm.cdf(d1)
                else:
                    delta = norm.cdf(d1) - 1

                deltas.append(delta)

            tsdf["delta"] = deltas

            return tsdf

        delta_df = compute_iv_delta(tsdf, date, spot)
        if right.upper() == 'CE':
            call_df = delta_df[delta_df['right'] == 'CE']
            call_df['delta_diff'] = (call_df['delta'] - delta).abs()
            if call_df['delta_diff'].isna().all() or call_df.empty:
                return {}
            best_row = call_df.loc[call_df['delta_diff'].idxmin()]
            delta_dict = {
                'symbol': best_row['symbol'],
                'o': best_row['o'],
                'h': best_row['h'],
                'l': best_row['l'],
                'c': best_row['c'],
                'v': best_row['v'],
                'oi': best_row['oi'],
                'strike': best_row['strike'],
                'right': best_row['right'],
                'delta': best_row['delta']
            }
            self.log.info(f'SUCCESSFULLY FOUND CALL TICKER {delta_dict}')
            return delta_dict if delta_dict else {}
        elif right.upper() == 'PE':
            put_df = delta_df[delta_df['right'] == 'PE']
            put_df = put_df.copy()
            put_df['delta'] = put_df['delta'].abs()
            put_df['delta_diff'] = (put_df['delta'] - delta).abs()
            if put_df['delta_diff'].isna().all() or put_df.empty:
                return {}
            best_row = put_df.loc[put_df['delta_diff'].idxmin()]
            
            delta_dict = {
                'symbol': best_row['symbol'],
                'o': best_row['o'],
                'h': best_row['h'],
                'l': best_row['l'],
                'c': best_row['c'],
                'v': best_row['v'],
                'oi': best_row['oi'],
                'strike': best_row['strike'],
                'right': best_row['right'],
                'delta': best_row['delta']
            }
            self.log.info(f'SUCCESSFULLY FOUND PUT TICKER {delta_dict}')
            return delta_dict if delta_dict else {}
        
    
    def find_ticker_by_strike(self, date, time, strike, right):
        timestampdf = self.get_all_ticks_by_timestamp(date, time)
        filtered_df = timestampdf[(timestampdf['strike'] == strike) & timestampdf['right'] == right]
        dict_to_return = {
                'symbol': filtered_df['symbol'],
                'o': filtered_df['o'],
                'h': filtered_df['h'],
                'l': filtered_df['l'],
                'c': filtered_df['c'],
                'v': filtered_df['v'],
                'oi': filtered_df['oi']
            }
        return dict_to_return

    def place_trade(self, timestamp, symbol, price, qty, cv, trade, system_action):
        return {
            'timestamp': timestamp,
            'symbol': symbol,
            'price': price,
            'qty': qty,
            'cv': cv,
            'trade': trade,
            'system_action': system_action
        }


