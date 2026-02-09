import duckdb
import pandas as pd

conn = duckdb.connect(r"C:\Users\Prahlad\Desktop\db\historical_db.ddb", read_only=True)
df = conn.execute("SELECT * FROM 'BANKNIFTY_I'").fetch_df()
print(df)
