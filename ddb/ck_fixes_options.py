import pandas as pd
import duckdb
import re
import datetime
import time

def parse_option_symbol(symbol: str):
        """Parses Through Option Tickers and extracts key details like Underlying, Expiry, Strike Etc."""

        pattern = r"""
            ^(?P<underlying>[A-Z0-9&\-]+?)  # underlying
            (?P<expiry>\d{8})               # YYMMDD
            (?P<strike>\d+(?:\.\d+)?)       # strike (int or decimal)
            (?P<right>CE|PE)$               # option type
        """

        m = re.match(pattern, symbol, re.VERBOSE)
        if not m:
            raise ValueError(f"Invalid option symbol: {symbol}")
        
        processed_symbol = m.groupdict()
        underlying = processed_symbol.get('underlying')
        return underlying


exception_list = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX', 'GOLD', 'CRUDEOIL', 'SILVER']
db = duckdb.connect(r"C:\Users\Admin\Desktop\db\historical_db.ddb")
tables = [t[0] for t in db.execute("SHOW TABLES").fetchall()]

filtered_tables = [
    table for table in tables
    if not any(exc in table for exc in exception_list)
]

for table in filtered_tables:

    # Ensure column exists
    db.execute(f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS underlying VARCHAR')

    # Only fetch rows still missing underlying
    df = db.execute(f"""
        SELECT symbol
        FROM "{table}"
        WHERE underlying IS NULL
    """).fetch_df()

    if df.empty:
        print(f"{table} already complete")
        continue

    start = time.time()

    df['underlying'] = df['symbol'].apply(parse_option_symbol)

    db.register("df_view", df)

    db.execute(f"""
        UPDATE "{table}" t
        SET underlying = d.underlying
        FROM df_view d
        WHERE t.symbol = d.symbol
    """)

    db.unregister("df_view")

    end = time.time()
    print(f"{table} resumed | Rows updated: {len(df)} | Time: {end-start:.2f}s")

