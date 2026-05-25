import datetime
import redis

r = redis.Redis('localhost', 6379, 0)
sessions = {
    'NIFTY': {
        'start': datetime.time(9, 15),
        'end': datetime.time(15, 29)
    },
    'SENSEX': {
        'start': datetime.time(9, 15),
        'end': datetime.time(15, 29)
    },
    'BANKNIFTY': {
        'start': datetime.time(9, 15),
        'end': datetime.time(15, 29)
    },

    'GOLD': {
        'start': datetime.time(9, 0),
        'end': datetime.time(23, 55)
    },

    'CRUDEOIL':  {
        'start': datetime.time(9, 0),
        'end': datetime.time(23, 55)
    }
}

db_paths = {
    'NSE': '',
    'BSE': '',
    'MCX': ''
}

continuous_codes = {
    '0': 'I',
    '1': 'II',
    '2': 'III',
    '3': 'IV'
}

lot_sizes = {
    'NIFTY': 65,
    'BANKNIFTY': 30,
    'MIDCPNIFTY': 120,
    'FINNIFTY': 60,
    'GOLD': 100,
    'CRUDEOIL': 100,
    'SENSEX': 20
}

strike_diff = {
    "NIFTY": 50,
    "BANKNIFTY": 100,
    "SENSEX": 100,
    "GOLD": 100,
    "CRUDEOIL": 50
}

index_components = {
    
  "NIFTY": [
    "ADANIENT",
    "ADANIPORTS",
    "APOLLOHOSP",
    "ASIANPAINT",
    "AXISBANK",
    "BAJAJ-AUTO",
    "BAJFINANCE",
    "BAJAJFINSV",
    "BEL",
    "BHARTIARTL",
    "CIPLA",
    "COALINDIA",
    "DRREDDY",
    "EICHERMOT",
    "ETERNAL",
    "GRASIM",
    "HCLTECH",
    "HDFCBANK",
    "HDFCLIFE",
    "HEROMOTOCO",
    "HINDALCO",
    "HINDUNILVR",
    "ICICIBANK",
    "INDUSINDBK",
    "INFY",
    "ITC",
    "JIOFIN",
    "JSWSTEEL",
    "KOTAKBANK",
    "LT",
    "M&M",
    "MARUTI",
    "NESTLEIND",
    "NTPC",
    "ONGC",
    "POWERGRID",
    "RELIANCE",
    "SBILIFE",
    "SBIN",
    "SHRIRAMFIN",
    "SUNPHARMA",
    "TATACONSUM",
    "TATAMOTORS",
    "TATASTEEL",
    "TCS",
    "TECHM",
    "TITAN",
    "TRENT",
    "ULTRACEMCO",
    "WIPRO"
  ],
  "BANKNIFTY": [
    "AUBANK",
    "AXISBANK",
    "BANDHANBNK",
    "BANKBARODA",
    "FEDERALBNK",
    "HDFCBANK",
    "ICICIBANK",
    "IDFCFIRSTB",
    "INDUSINDBK",
    "KOTAKBANK",
    "PNB",
    "SBIN"
  ],
  "NIFTYNXT50": [
    "ABB",
    "ADANIENSOL",
    "ADANIGREEN",
    "ADANIPOWER",
    "AMBUJACEM",
    "BAJAJHLDNG",
    "BANKBARODA",
    "BPCL",
    "BRITANNIA",
    "BOSCHLTD",
    "CANBK",
    "CGPOWER",
    "CHOLAFIN",
    "DABUR",
    "DLF",
    "DIVISLAB",
    "GAIL",
    "GODREJCP",
    "HAL",
    "HAVELLS",
    "HYUNDAI",
    "ICICIGI",
    "ICICIPRULI",
    "INDIANHOTEL",
    "INDIGO",
    "INDIANB",
    "IOC",
    "JINDALSTEL",
    "LICI",
    "LODHA",
    "MOTHERSON",
    "NAUKRI",
    "NMDC",
    "PFC",
    "PIDILITIND",
    "PNB",
    "RECLTD",
    "SHREECEM",
    "SIEMENS",
    "SWIGGY",
    "TORNTPHARM",
    "TVSMOTOR",
    "UNITDSPR",
    "VBL",
    "VEDL",
    "ZYDUSLIFE",
    "HINDPETRO",
    "IRFC",
    "DMART",
    "BAJAJHFL"
  ]
}