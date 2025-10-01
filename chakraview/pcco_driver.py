import multiprocessing
from chakraview.pcco_test import PCCO

# This function will run in each process
def run_backtest(uid):
    # Instantiate PCCO inside the process
    p = PCCO()
    # Set up and run backtest
    p.create_backtest(uid)
    print(f"Backtest finished for {uid}")

if __name__ == "__main__":
    # List of UIDs to backtest
    minute_list = [3, 4, 5, 6, 7, 8]
    uids = [f"PCCO_nifty_{minute}" for minute in minute_list]

    batch_size = 5
    
    for i in range(0, len(uids), batch_size):
        batch = uids[i:i + batch_size]
        processes = []

        for uid in batch:
            proc = multiprocessing.Process(target=run_backtest, args=(uid,))
            proc.start()
            processes.append(proc)

        # Wait for this batch to finish
        for proc in processes:
            proc.join()

    print("All backtests completed.")

