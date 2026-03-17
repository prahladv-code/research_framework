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