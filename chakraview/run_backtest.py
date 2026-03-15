from chakraview.VWAPTRAIL import VWAP
import multiprocessing as mp

# ma_iterations = list(range(1, 100))
ma_iterations = []
underlyings = ['NIFTY'] #['GOLD', 'CRUDEOIL'] 
timeframe_iterations = [5]
# multiplier_iterations = [1, 1.5, 2, 2.5, 3, 3.5, 4]
uids = ["VWAP_GOLD_0_5_60_3",
        "VWAP_CRUDEOIL_0_5_60_3",
        "VWAP_NIFTY_0_5_16_3",
        "VWAP_NIFTY_0_5_30_3",
        "VWAP_NIFTY_0_5_40_3",
        "VWAP_NIFTY_0_5_50_3",
        "VWAP_NIFTY_0_5_60_3",
        "VWAP_NIFTY_0_5_63_3",
        "VWAP_NIFTY_0_5_33_3",
        "VWAP_NIFTY_0_5_93_3",
        "VWAP_NIFTY_0_5_70_3",
        "VWAP_NIFTY_0_5_80_3",
        "VWAP_NIFTY_0_5_30_2",
        "VWAP_NIFTY_0_5_60_2",
        "VWAP_NIFTY_0_5_90_2",
        "VWAP_NIFTY_0_5_20_2"
        ] 
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
    create_processes(uids, 3)


# "VWAP_NIFTY_0_1_0_0"