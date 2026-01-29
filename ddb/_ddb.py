import duckdb
import pandas as pd


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


    def _table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in DuckDB.
        """
        result = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [table_name]
        ).fetchone()[0]
        return result > 0

    def process_expiry_tables(self, df: pd.DataFrame):
        """
        Create or append expiry-wise tables named as:
        expiry_YYYY-MM-DD
        """

        # Ensure expiry is string-friendly (important!)
        df = df.copy()
        df["expiry"] = pd.to_datetime(df["expiry"]).dt.strftime("%Y-%m-%d")

        for expiry, expiry_df in df.groupby("expiry"):
            table_name = f'expiry_{expiry}'

            # Register temp dataframe
            self.conn.register("df_tmp", expiry_df)

            if not self._table_exists(table_name):
                # Create new table
                self.conn.execute(f"""
                    CREATE TABLE "{table_name}" AS
                    SELECT * FROM df_tmp
                """)
            else:
                # Append to existing table
                self.conn.execute(f"""
                    INSERT INTO "{table_name}"
                    SELECT * FROM df_tmp
                """)

            self.conn.unregister("df_tmp")
    
    def process_underlyings(self, df: pd.DataFrame, db_name: str):

        """Creates Tables In DuckDB Containing Data For All Underlyings (Spot) Data"""
        self.conn.register('df_temp', df)

        if not self._table_exists(db_name):
            self.conn.execute(f"""CREATE TABLE "{db_name}" AS SELECT * FROM df_temp""")
        else:
            self.conn.execute(f"""INSERT INTO "{db_name}" SELECT * FROM df_temp""")
        
        self.conn.unregister('df_temp')
    
    def process_futures(self, df: pd.DataFrame, db_name: str):

        """Creates Tables In DuckDB Containing Futures Data For All Underlyings"""
        self.conn.register('df_temp', df)

        if not self._table_exists(db_name):
            self.conn.execute(f"""CREATE TABLE "{db_name}" AS SELECT * FROM df_temp""")
        else:
            self.conn.execute(f"""INSERT INTO "{db_name}" SELECT * FROM df_temp""")
        
        self.conn.unregister('df_temp')
        
    
