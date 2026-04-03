from chakraview.BTST import BTST
import multiprocessing as mp

# ma_iterations = list(range(1, 100))
ma_iterations = []
underlyings = ['NIFTY'] #['GOLD', 'CRUDEOIL'] 
timeframe_iterations = [5]
# multiplier_iterations = [1, 1.5, 2, 2.5, 3, 3.5, 4]
uids = [
        'BTST_NIFTY_1_210_0.5_0_0',
        'BTST_NIFTY_1_0_1.5_0_0',
        ]

# for ma in ma_iterations:
#     for timeframe in timeframe_iterations:
#             # You can use a static method or small helper
#         for underlying in underlyings:
#             uid = f"PRICEMACLOSEFILTER_{underlying}_0_{ma}_{timeframe}_False"
#             # uids.append(uid)

def run_backtest(uid):
    backtest = BTST()         # instantiate here (not outside)
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
        # "BTST_NIFTY_1_0_0_0"


# ['VWAP_ADANIENT_5_60_3',
#  'VWAP_ADANIPORTS_5_60_3',
#  'VWAP_APOLLOHOSP_5_60_3',
#  'VWAP_ASIANPAINT_5_60_3',
#  'VWAP_AXISBANK_5_60_3',
#  'VWAP_BAJAJ-AUTO_5_60_3',
#  'VWAP_BAJFINANCE_5_60_3',
#  'VWAP_BAJAJFINSV_5_60_3',
#  'VWAP_BEL_5_60_3',
#  'VWAP_BHARTIARTL_5_60_3',
#  'VWAP_CIPLA_5_60_3',
#  'VWAP_COALINDIA_5_60_3',
#  'VWAP_DIVISLAB_5_60_3',
#  'VWAP_DRREDDY_5_60_3',
#  'VWAP_EICHERMOT_5_60_3',
#  'VWAP_GRASIM_5_60_3',
#  'VWAP_HCLTECH_5_60_3',
#  'VWAP_HDFCBANK_5_60_3',
#  'VWAP_HDFCLIFE_5_60_3',
#  'VWAP_HEROMOTOCO_5_60_3',
#  'VWAP_HINDALCO_5_60_3',
#  'VWAP_HINDUNILVR_5_60_3',
#  'VWAP_ICICIBANK_5_60_3',
#  'VWAP_INDUSINDBK_5_60_3',
#  'VWAP_INFY_5_60_3',
#  'VWAP_ITC_5_60_3',
#  'VWAP_JIOFIN_5_60_3',
#  'VWAP_JSWSTEEL_5_60_3',
#  'VWAP_KOTAKBANK_5_60_3',
#  'VWAP_LT_5_60_3',
#  'VWAP_M&M_5_60_3',
#  'VWAP_MARUTI_5_60_3',
#  'VWAP_NESTLEIND_5_60_3',
#  'VWAP_NTPC_5_60_3',
#  'VWAP_ONGC_5_60_3',
#  'VWAP_POWERGRID_5_60_3',
#  'VWAP_RELIANCE_5_60_3',
#  'VWAP_SBILIFE_5_60_3',
#  'VWAP_SBIN_5_60_3',
#  'VWAP_SHRIRAMFIN_5_60_3',
#  'VWAP_SUNPHARMA_5_60_3',
#  'VWAP_TATACONSUM_5_60_3',
#  'VWAP_TATAMOTORS_5_60_3',
#  'VWAP_TATASTEEL_5_60_3',
#  'VWAP_TCS_5_60_3',
#  'VWAP_TECHM_5_60_3',
#  'VWAP_TITAN_5_60_3',
#  'VWAP_ULTRACEMCO_5_60_3',
#  'VWAP_WIPRO_5_60_3',
#  'VWAP_ZOMATO_5_60_3'] 

# 'IVIX_NIFTY_30_7_20_10_True_3_0',
# 'IVIX_NIFTY_30_7_15_5_True_3_0',
# 'IVIX_NIFTY_30_7_25_15_True_3_0',
# 'IVIX_NIFTY_60_7_20_10_True_3_0',
# 'IVIX_NIFTY_60_7_15_5_True_3_0',
# 'IVIX_NIFTY_60_7_25_15_True_3_0',

# "BTST_NIFTY_1_1_0_0_0",
# "BTST_SENSEX_1_1_0_0_0",
# "BTST_NIFTY_1_2_0_0_0",
# "BTST_SENSEX_1_2_0_0_0"

# 'AVWAP_NIFTY_25_W_0_0'
# 'AVWAP_NIFTY_25_D_0_0',
# 'AVWAP_NIFTY_25_M_0_0',
# 'AVWAP_NIFTY_25_Q_0_0'
# 'AVWAP_NIFTY_25_M_3_0',
# 'AVWAP_BANKNIFTY_25_M_3_0',
# 'AVWAP_NIFTY_25_W_0_0',

# 'BTSTOI_NIFTY_1_1.3_0.7_0_0',
# 'BTSTOI_SENSEX_1_1.2_0.8_0_0',
# 'BTSTOI_BANKNIFTY_1_1.2_0.8_0_0',
# 'BTSTOI_NIFTY_1_1.2_0.8_0_0',
# 'BTSTOI_NIFTY_1_1.5_0.5_0_0',
# 'BTST_SENSEX_1_1.2_0.8_0_0',
# 'BTST_NIFTY_1_1.2_0.8_0_0',