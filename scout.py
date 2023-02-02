# generic imports
import time
import os
import sys
import pandas as pd
import numpy as np
import re
import math
import datetime
import threading
import queue

# scrapping requests
import wikipedia
import yfinance
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras
import pyasx.data.companies
from tqdm import tqdm


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def millify(n):
    millnames = ['', 'K', 'M', 'B', 'T']
    n = float(n)
    millidx = max(0, min(len(millnames) - 1,
                         int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3))))

    if millidx <= 4:
        suffix = millnames[millidx]
    else:
        suffix = ' * 10^{0}'.format(millidx * 3)

    return '${:.1f}{}'.format(n / 10 ** (3 * millidx), suffix)


class Database:
    def __init__(self):
        self.database = "invest_simple"
        self.host = "139.130.195.51"  # "192.168.15.200"
        self.user = "pi"  # "interface_account"
        self.password = "actionN06"
        self.port = "5432"

        # define connection and cursor for later use
        self.connection = None
        self.cursor = None

    def database_connect(self):
        # attempt to connect to database
        try:
            # connect to database
            connection = psycopg2.connect(database=self.database,
                                          host=self.host,
                                          user=self.user,
                                          password=self.password,
                                          port=self.port)

            # get cursor
            cursor = connection.cursor()
        except psycopg2.OperationalError as e:
            self.err_print(function=None, msg='Failed to establish database connection. Fatal-error. Exiting program', error=e)
        except (Exception, psycopg2.DatabaseError) as e:
            self.err_print(function=None, msg='msg1', error=e)
            exit()

        # if self.conn is not None: self.conn.close()
        return connection, cursor

    def query(self, query, cursor):
        # query all data from cashflow
        cursor.execute(query)

        # return results
        return cursor.fetchall()

    def scout_stocks(self):
        conn, cur = self.database_connect()

        sql = """SELECT t.id, LEFT(t.name, 24), t.marketcap, t.intrinsicvalue_best, i.growth_rate_2a, i.growth_rate_2b, t.sector 
                FROM ticker t 
                LEFT JOIN intrinsicvalue i ON i.id=t.id 
                WHERE shares_outstanding > 0 AND t.marketcap > 50 AND update_attempts = 0 
                    AND 0.90*t.marketcap < t.intrinsicvalue_best
                    AND i.growth_rate_2a > -0.1 AND i.growth_rate_2a >= i.growth_rate_2b
                    AND (SELECT COUNT(1) FROM cashflow WHERE cashflow.id=t.id) > 3
                ORDER BY t.intrinsicvalue_best DESC;"""

        sql = """SELECT t.id, LEFT(t.name, 24), t.marketcap, t.intrinsicvalue_best, i.growth_rate_2a, i.growth_rate_2b, t.sector, (SELECT COUNT(1) FROM cashflow WHERE cashflow.id=t.id) AS DataSize
                FROM ticker t 
                LEFT JOIN intrinsicvalue i ON i.id=t.id 
                WHERE shares_outstanding > 0 AND t.favourite=TRUE
                ORDER BY (t.intrinsicvalue_best::FLOAT/t.marketcap) DESC;"""

        #sql = """SELECT id FROM intrinsicvalue WHERE ROUND(CAST(growth_rate_2a AS numeric),6)=0.195084 ORDER BY growth_rate_2a DESC;"""
        results = self.query(sql, cursor=cur)
        #print(results)
        print('found {} items'.format(len(results)))
        #return
        time.sleep(1)
        for item in results:
            if 100*((item[3]/item[2])-1) > 0: col = bcolors.OKGREEN
            else: col = bcolors.WARNING
            print('Ticker: {} | {} | {} | {}  -> {} ({}) | Revenue {} | Capex {} | {} yrs of data'.format(
                item[0].ljust(7),
                item[1].ljust(24),
                item[6][:18].ljust(18),
                millify(item[2]*1000000).rjust(7), millify(item[3]*1000000).rjust(7),
                col + '{:+.1f}%'.format(100*((item[3]/item[2])-1)).rjust(8) + bcolors.ENDC,
                '{:+.1f}%'.format(item[4]*100).rjust(6).ljust(7),
                '{:+.1f}%'.format(item[5]*100).rjust(6).ljust(7),
                item[7]))
            time.sleep(0.55)

        return
        tempst = ''
        for item in results:
            tempst = tempst + "('{}',),".format(item[0])

        print('[' + tempst + ']')
        return
        s = time.time()
        dur = 30*60
        f = s + dur

        while time.time() < f:
            print('[{}] finding'.format(datetime.datetime.now().strftime("%H:%M:%S.%f")[:-4]))

            time.sleep(0.5)


def main():
    # create database object
    pi_sql = Database()

    pi_sql.scout_stocks()


if __name__ == '__main__':
    main()