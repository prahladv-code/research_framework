from chakraview.VWAP_STOCKS import VWAP
import multiprocessing as mp

# ma_iterations = list(range(1, 100))
ma_iterations = []
underlyings = ['NIFTY'] #['GOLD', 'CRUDEOIL'] 
timeframe_iterations = [5]
# multiplier_iterations = [1, 1.5, 2, 2.5, 3, 3.5, 4]
uids = ['VWAP_ADANIENT_5',
 'VWAP_ADANIPORTS_5',
 'VWAP_APOLLOHOSP_5',
 'VWAP_ASIANPAINT_5',
 'VWAP_AXISBANK_5',
 'VWAP_BAJAJ-AUTO_5',
 'VWAP_BAJFINANCE_5',
 'VWAP_BAJAJFINSV_5',
 'VWAP_BEL_5',
 'VWAP_BHARTIARTL_5',
 'VWAP_CIPLA_5',
 'VWAP_COALINDIA_5',
 'VWAP_DIVISLAB_5',
 'VWAP_DRREDDY_5',
 'VWAP_EICHERMOT_5',
 'VWAP_GRASIM_5',
 'VWAP_HCLTECH_5',
 'VWAP_HDFCBANK_5',
 'VWAP_HDFCLIFE_5',
 'VWAP_HEROMOTOCO_5',
 'VWAP_HINDALCO_5',
 'VWAP_HINDUNILVR_5',
 'VWAP_ICICIBANK_5',
 'VWAP_INDUSINDBK_5',
 'VWAP_INFY_5',
 'VWAP_ITC_5',
 'VWAP_JIOFIN_5',
 'VWAP_JSWSTEEL_5',
 'VWAP_KOTAKBANK_5',
 'VWAP_LT_5',
 'VWAP_M&M_5',
 'VWAP_MARUTI_5',
 'VWAP_NESTLEIND_5',
 'VWAP_NTPC_5',
 'VWAP_ONGC_5',
 'VWAP_POWERGRID_5',
 'VWAP_RELIANCE_5',
 'VWAP_SBILIFE_5',
 'VWAP_SBIN_5',
 'VWAP_SHRIRAMFIN_5',
 'VWAP_SUNPHARMA_5',
 'VWAP_TATACONSUM_5',
 'VWAP_TATAMOTORS_5',
 'VWAP_TATASTEEL_5',
 'VWAP_TCS_5',
 'VWAP_TECHM_5',
 'VWAP_TITAN_5',
 'VWAP_ULTRACEMCO_5',
 'VWAP_WIPRO_5',
 'VWAP_ZOMATO_5'] 
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
    create_processes(uids, 5)


# "VWAP_NIFTY_0_1_0_0"
# "BOLLINGERBANDS_NIFTY_0_25_60_2_1.5"

        #  "PRICEMABANDS_NIFTY_0_33_25_1",
        # "PRICEMABANDS_NIFTY_0_63_25_1",
        # "PRICEMABANDS_NIFTY_0_93_25_1",
        # "BOLLINGERBANDS_NIFTY_0_25_30_2_0.1",
        # "BOLLINGERBANDS_NIFTY_0_25_90_2_0.1",
        # "BOLLINGERBANDS_NIFTY_0_25_30_1.5_0.1",
        # "BOLLINGERBANDS_NIFTY_0_25_60_1.5_0.1",
        # "BOLLINGERBANDS_NIFTY_0_25_90_1.5_0.1",
        # "BOLLINGERBANDS_NIFTY_0_25_30_2_0.05",
        # "BOLLINGERBANDS_NIFTY_0_25_60_2_0.05",
        # "BOLLINGERBANDS_NIFTY_0_25_90_2_0.05",
        # "BOLLINGERBANDS_NIFTY_0_25_30_1.5_0.05",
        # "BOLLINGERBANDS_NIFTY_0_25_60_1.5_0.05",
        # "BOLLINGERBANDS_NIFTY_0_25_90_1.5_0.05",