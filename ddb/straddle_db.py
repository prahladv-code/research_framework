from ddb._ddb import Ddb
import time

if __name__ == '__main__':
    start = time.time()
    db = Ddb(r"C:\Users\Admin\Desktop\db\historical_db.ddb")
    straddle_df = db.generate_straddle_df('NIFTY', 0)
    db.process_underlyings(straddle_df, 'NIFTY_straddles')
    end = time.time()
    print(f'Elapsed Time In Ingesting Straddle DF: {end - start}')
