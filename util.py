import math
import threading
from datetime import datetime as dt


class ThreadSafeCounter:
    # constructor
    def __init__(self):
        # initialize counter
        self._counter = 0
        # initialize lock
        self._lock = threading.Lock()

    def read(self):
        with self._lock:
            return self._counter

    # increment the counter
    def increment(self):
        with self._lock:
            self._counter += 1

    def set(self, value):
        with self._lock:
            self._counter = value

    # increment and get the counter value
    def read_increment(self):
        with self._lock:
            self._counter += 1
            return self._counter


class ThreadSafeFlag:
    # constructor
    def __init__(self):
        # initialize counter
        self._flag = False
        # initialize lock
        self._lock = threading.Lock()

    def value(self):
        with self._lock:
            return self._flag

    def set(self, value: bool):
        with self._lock:
            self._flag = value


class thread_print():
    def __init__(self):
        # initialize lock
        self._lock = threading.Lock()

    def print(self, msg, no_format=False):
        with self._lock:
            if not no_format:
                print('[{time_formatted}] {message}'.format(time_formatted=self.time_str(), message=msg))
            else:
                print('{message}'.format(message=msg))

    @staticmethod
    def time_str():
        return dt.now().strftime("%H:%M:%S.%f")[:-3]


def print_subitems(obj):
    print()

    try:
        for key in obj.keys():
            print("obj::{}: {}".format(key, obj[key]))
    except (AttributeError, ) as e:
        for item in obj:
            print("obj::{}".format(item))


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    SYSTEM = '\033[35m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class Queue:
    Done = 'QueueComplete'
    MsgDone = 'MsgComplete'


def millify(n):
    millnames = ['', 'K', 'M', 'B', 'T']
    n = float(n)
    millidx = max(0, min(len(millnames) - 1,
                         int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3))))

    if millidx <= 4:
        suffix = millnames[millidx]
    else:
        suffix = ' * 10^{0}'.format(millidx * 3)

    return '${:.2f}{}'.format(n / 10 ** (3 * millidx), suffix)
