import duckdb
import pandas as pd

conn = duckdb.connect(r"C:\Users\Prahlad\Desktop\db\historical_data.ddb", read_only=True)
df = conn.execute("SELECT * FROM '2025-09-02'").fetch_df()
print(df)
