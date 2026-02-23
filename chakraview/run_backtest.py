from chakraview.VWAP import VWAP
import multiprocessing as mp

# ma_iterations = list(range(1, 100))
ma_iterations = []
underlyings = ['NIFTY'] #['GOLD', 'CRUDEOIL'] 
timeframe_iterations = [1]
# multiplier_iterations = [1, 1.5, 2, 2.5, 3, 3.5, 4]
uids = ["VWAP_NIFTY_0_1_0_0"]
# for ma in ma_iterations:
#     for timeframe in timeframe_iterations:
#             # You can use a static method or small helper
#         for underlying in underlyings:
#             uid = f"PRICEMACLOSEFILTER_{underlying}_0_{ma}_{timeframe}_False"
#             # uids.append(uid)

def run_backtest(uid):
    backtest = VWAP()         # instantiate here (not outside)
    backtest.run_backtest(uid)

def create_processes(uid_list, batch_size):
    for i in range(0, len(uid_list), batch_size):
        processes = []

        batch = uid_list[i : i + batch_size]

        for uid in batch:
            p = mp.Process(target=run_backtest, args=(uid,))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

if __name__ == '__main__':
    create_processes(uids, 10)


# "VWAP_NIFTY_0_1_0_0"