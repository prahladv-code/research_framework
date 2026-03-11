
import pandas as pd
import duckdb

db = duckdb.connect(r"C:\Users\Admin\Desktop\db\historical_db.ddb")
underlyings = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX', 'GOLD', 'CRUDEOIL', 'SILVER']

for underlying in underlyings:
    db.execute(f"""
        ALTER TABLE {underlying}_I
        ADD COLUMN IF NOT EXISTS underlying VARCHAR
    """)
    db.execute(f"""
        UPDATE {underlying}_I
        SET underlying = '{underlying}'
    """)
    
