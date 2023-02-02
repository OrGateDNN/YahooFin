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
from tkinter import*

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

    return '${:.2f}{}'.format(n / 10 ** (3 * millidx), suffix)


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

        self.colour_palette = {'background': 'light grey'}

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

    def interface(self):

        window = Tk()
        window.title("Welcome to TutorialsPoint")
        window.geometry('400x400')
        window.configure(background=self.colour_palette['background']);

        a = Label(window, text="First Name", background=self.colour_palette['background']).grid(row=0, column=0)
        b = Label(window, text="Last Name").grid(row=1, column=0)
        c = Label(window, text="Email Id").grid(row=2, column=0)
        d = Label(window, text="Contact Number").grid(row=3, column=0)
        a1 = Entry(window).grid(row=0, column=1)
        b1 = Entry(window).grid(row=1, column=1)
        c1 = Entry(window).grid(row=2, column=1)
        d1 = Entry(window).grid(row=3, column=1)

        def clicked():
            res = "Welcome to piworld"
            #lbl.configure(text=res)

        btn = Button(window, text="Submit").grid(row=4, column=0)
        window.mainloop()

def main():
    # create database object
    pi_sql = Database()

    pi_sql.interface()


if __name__ == '__main__':
    main()