import duckdb
import pandas as pd

conn = duckdb.connect(r"C:\Users\Admin\Desktop\db\historical_db.ddb", read_only=True)
df = conn.execute("SELECT * FROM 'NIFTY_I'").fetch_df()
print(df)
