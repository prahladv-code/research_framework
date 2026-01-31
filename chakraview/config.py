import datetime
import redis

r = redis.Redis('localhost', 6379, 0)
sessions = {
    'nifty': {
        'start': datetime.time(9, 15),
        'end': datetime.time(15, 29)
    },
    'nifty_fut': {
        'start': datetime.time(9, 15),
        'end': datetime.time(15, 29)
    },
    'commodities': {
        'start': datetime.time(9, 0),
        'end': datetime.time(23, 30)
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
    'CRUDEOIL': 100
}