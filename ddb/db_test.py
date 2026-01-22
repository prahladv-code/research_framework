import duckdb
import pandas as pd

conn = duckdb.connect(r"C:\Users\Prahlad\Desktop\NSE_DB\nse_db.ddb", read_only=True)
df = conn.execute("SELECT * FROM 'expiry_2025-09-30'").fetch_df()
print(df)
