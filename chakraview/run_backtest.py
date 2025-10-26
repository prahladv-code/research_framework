from chakraview.PRICEMA import PRICEMA
import multiprocessing as mp

ma_iterations = [33, 63, 93]
timeframe_iterations = [25]

uids = []
for ma in ma_iterations:
    for timeframe in timeframe_iterations:
        # You can use a static method or small helper
        uid = f"PRICEMA_niftyfut_{ma}_{timeframe}_False"
        uids.append(uid)

def run_backtest(uid):
    backtest = PRICEMA()         # instantiate here (not outside)
    backtest.create_backtest(uid)

def create_processes(uid_list):
    processes = []
    for uid in uid_list:
        p = mp.Process(target=run_backtest, args=(uid,))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()

if __name__ == '__main__':
    create_processes(uids)
