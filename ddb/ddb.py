import duckdb

class Ddb:
    def __init__(self, db_path: str):
        """
        Create And Process DuckDb Database For Historical Data Processing.

        """
        self.conn = duckdb.connect(db_path)
    
    def process_daily_tables(self, df):
        pass

    def process_expiry_tables(self, df):
        pass
    