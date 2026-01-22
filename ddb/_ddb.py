import duckdb

class Ddb:
    def __init__(self, db_path: str):
        """
        Create and process DuckDB database for historical data processing.
        """
        self.conn = duckdb.connect(db_path)

    def process_daily_tables(self, df, db_name: str):
        """
        Create a table named `db_name` from a pandas DataFrame.
        """
        # Register the DataFrame as a temporary view
        self.conn.register("df_tmp", df)

        # Create table if it does not exist
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS "{db_name}" AS
            SELECT * FROM df_tmp
        """)

        # Optional: unregister after use
        self.conn.unregister("df_tmp")

    def process_expiry_tables(self, df):
        pass
