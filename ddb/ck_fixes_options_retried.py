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

pattern = r'^([A-Z0-9&\-]+?)\d{8}\d+(?:\.\d+)?(CE|PE)$'

for table in filtered_tables:

    db.execute(f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS underlying VARCHAR')

    remaining = db.execute(f"""
        SELECT COUNT(*) 
        FROM "{table}" 
        WHERE underlying IS NULL
    """).fetchone()[0]

    if remaining == 0:
        print(f"{table} already complete")
        continue

    start = time.time()

    db.execute(f"""
        UPDATE "{table}"
        SET underlying = regexp_extract(symbol, '{pattern}', 1)
        WHERE underlying IS NULL
    """)

    end = time.time()

    print(f"{table} updated | Rows: {remaining} | Time: {end-start:.2f}s")

db.execute("CHECKPOINT")

