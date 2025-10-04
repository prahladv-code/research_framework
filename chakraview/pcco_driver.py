import multiprocessing
from chakraview.pcco_opt import PCCO

# This function will run in each process
def run_backtest(uid):
    # Instantiate PCCO inside the process
    p = PCCO()
    # Set up and run backtest
    p.create_backtest(uid)
    print(f"Backtest finished for {uid}")

if __name__ == "__main__":
    # List of UIDs to backtest
    minute_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tgt_pct = [0.02, 0.03, 0.04, 0.05, 0.06]
    uids = []
    for m in minute_list:
        for tgt in tgt_pct:
            uids.append(f'PCCOOPT_nifty_{m}_{tgt}_False_0')


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

