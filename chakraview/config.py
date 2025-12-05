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
        'end': datetime.time(15, 0)
    }
}