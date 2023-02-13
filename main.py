# generic imports
import time
import os
import sys
import pandas as pd
import numpy as np
import re
import math
import queue
from threading import Thread

# scrapping requests
import wikipedia
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras
import pyasx.data.companies
#from tqdm import tqdm

# import local files
from util import ThreadSafeFlag, ThreadSafeCounter, Queue, bcolors, thread_print
from yahoo_interface import yfin_obj, yquery_obj

HourlyLimit = 180

_discount_rate = 0.15
_growth_perpetuity = 0.025


class FailedTickerAnalysis(Exception):
    "Raised on failed ticker analysis"

    def __init__(self, message="", stock_anaylsis_failure=True):
        self.stock_anaylsis_failure = stock_anaylsis_failure
        self.message = message

        super().__init__(self.message)


class Database:
    def __init__(self, collector_pool_size: int = 1):
        self.database = "invest_simple"
        self.host = "139.130.195.51"  # "192.168.15.200"
        self.user = "pi"  # "interface_account"
        self.password = "actionN06"
        self.port = "5432"

        # define connection and cursor for later use
        self.connection = None
        self.cursor = None

        # define how often each stock index is refreshed
        self._refresh_asx = 8 * 60 * 60
        self._refresh_nas = 8 * 60 * 60
        self._refresh_hke = 8 * 60 * 60

        # define query limits
        self._max_query_per_sec = 3600 / HourlyLimit  # 400calls/hour -> 400calls/3600s -> 3600/400 -> 7.5s per call
        self._max_idle_ticker_queue_size = 20

        self._next_valid_query = time.time() + self._max_query_per_sec

        # define constraint definitions for query period
        self.standard_update_period = 21  # re-poll period (days) for normal conditions
        self.failed_retry_update_period = 42  # re-poll period (days) if failed
        self.ultimate_failed_update_period = 186  # re-poll period (days) if failed too many times
        self.retry_attempts = 12  # number of re-poll attempts before listing as low priority

        # storage for threads start time
        self.time_start = None

        # collector pool size
        self.collector_pool_size = collector_pool_size
        self.print_handle = thread_print()

        # setup dict of queues and arrays
        self.queues = {}
        self.counters = {}
        self.threads = {}
        self.flags = {}

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
            self.err_print(function=None, msg='Failed to establish database connection. Fatal-error. Exiting program',
                           error=e)
        except (Exception, psycopg2.DatabaseError) as e:
            self.err_print(function=None, msg='msg1', error=e)
            exit()

        # if self.conn is not None: self.conn.close()
        return connection, cursor

    def setup(self):
        # setup queues
        self.queues['ticker_list'] = queue.Queue()
        self.queues['ticker_data'] = queue.Queue()
        self.queues['message_logs'] = queue.Queue()

        # setup counters
        self.counters['tasks_sent'] = ThreadSafeCounter()
        self.counters['tasks_awaiting'] = ThreadSafeCounter()

        # setup flags
        self.flags['finder_exit_flag'] = ThreadSafeFlag()
        self.flags['collector_exit_flag'] = ThreadSafeFlag()
        self.flags['processor_exit_flag'] = ThreadSafeFlag()
        self.flags['logger_exit_flag'] = ThreadSafeFlag()
        self.flags['e_stop_flag'] = ThreadSafeFlag()

        # setup threads
        # -- FINDER
        self.threads['Finder'] = Thread(target=self.find_stocks,
                                        args=(self.flags['finder_exit_flag'], self.queues['ticker_list'],
                                              self.queues['message_logs'], self.counters['tasks_awaiting']))

        # -- COLLECTOR
        for index in range(1, self.collector_pool_size + 1):
            self.threads['Collector_{}'.format(index)] = Thread(target=self.collector, args=(
                self.flags['collector_exit_flag'], self.queues['ticker_list'], self.queues['ticker_data'],
                (self.counters['tasks_sent'], self.counters['tasks_awaiting']), self.queues['message_logs']))

        # -- PROCESSOR
        self.threads['Processor'] = Thread(target=self.process_data,
                                           args=(self.flags['processor_exit_flag'], self.queues['ticker_data'],
                                                      self.queues['message_logs']))
        # -- LOGGER
        self.threads['Logger'] = Thread(target=self.logger,
                                        args=(self.flags['logger_exit_flag'], self.queues['message_logs'],))

    def start(self, timeout: int = -1, silent=False):
        self.setup()

        # for simplicity
        msg_queue = self.queues['message_logs']

        # set start time and then start all threads
        self.time_start = time.time()

        # start threads
        for thread_name in self.threads.keys():
            if thread_name[:9] == 'Collector':
                fname = 'Collector'
            else:
                fname = thread_name
            self.msg(msg_queue, 'Starting{}Thread'.format(fname), "\tstarting {} thread: ".format(thread_name), style='system')
            self.threads[thread_name].start()

        # wait for all threads to finish
        for thread_name in self.threads:
            while self.threads[thread_name].is_alive():
                # try for 10 seconds to join thread
                for _ in range(15):
                    self.threads[thread_name].join(2)

                if timeout > 0:
                    if time.time() - self.time_start > timeout or self.flags['e_stop_flag'].value():
                        self.stop()
                    elif not silent:
                        self.print_time_remaining(timeout, msg_queue)

    def stop(self):
        # for simplicity
        msg_queue = self.queues['message_logs']

        self.msg(msg_queue, 'ShutdownCommand', 'Received shutdown command. Safely closing threads', style='fail', nowait=True)

        # shutdown stock finder queue
        self.msg(msg_queue, 'FinderThreadShutdown', '(awaiting) {} process to close gracefully'.format('finder'), style='system')
        self.flags['finder_exit_flag'].set(True)
        self.threads['Finder'].join()

        # clear unstarted tasks
        self.msg(msg_queue, 'ClearingQueues', 'clearing queues', style='system')
        for queue_name in self.queues.keys():
            if queue_name == 'message_logs':
                continue

            self.msg(msg_queue, 'ClearingQueues', '\t  clearing {} queue'.format(queue_name), style='system')

            # while queue not empty, get next item until all items cleared
            while not self.queues[queue_name].empty():
                try:
                    self.queues[queue_name].get(block=False)
                    self.queues[queue_name].task_done()
                except queue.Empty:
                    continue

        # set msg send
        self.msg(msg_queue, 'ClearingQueues', Queue.MsgDone)

        # wait for collector processors to end
        self.flags['collector_exit_flag'].set(True)
        for thread_name in [key for key in self.threads.keys() if key[:9] == 'Collector']:
            # self.flags['processor_exit_flag'] = True
            #         self.flags['logger_exit_flag'] = True
            self.msg(msg_queue, 'CollectorThreadShutdown', '(awaiting) {} process to close gracefully'.format(thread_name), style='system')
            self.threads[thread_name].join()

        # wait for processor functions to end
        self.flags['processor_exit_flag'].set(True)
        self.msg(msg_queue, 'ProcessorThreadShutdown', '(awaiting) {} process to close gracefully'.format('processor'), style='system')
        self.threads['Processor'].join()

        self.flags['logger_exit_flag'].set(True)
        self.msg(msg_queue, 'LoggerThreadShutdown', '(awaiting) {} process to close gracefully'.format('logger'), style='system')
        self.threads['Logger'].join()

    def find_stocks(self, thread_exit_flag: bool, ticker_queue, msg_queue, prefetched_tasks):
        self.msg(msg_queue, 'StartingFinderThread', '\t\tdone.. finder thread started', style='system', nowait=True)

        connection, cursor = self.database_connect()
        last_update = -1
        update_asx = time.time() + self._refresh_asx
        update_nas = time.time() + self._refresh_nas
        update_hke = time.time() + self._refresh_hke
        update_refresh = -1

        while True:
            if thread_exit_flag.value():
                self.msg(msg_queue, 'FinderThreadShutdown', '\t\tDone.. finder thread stopped', 'system', nowait=True)
                return

            # add new asx stocks
            if time.time() >= update_asx:
                self.update_asx_list(cursor=cursor, connection=connection)
                update_asx = time.time() + self._refresh_asx
                self.msg(msg_queue, 'FinderProcess', 'FinderProcess: refreshed new asx items', nowait=True)

            if time.time() >= update_nas:
                self.update_nasdaq_list(cursor=cursor, connection=connection, msg_queue=msg_queue)
                update_nas = time.time() + self._refresh_nas
                self.msg(msg_queue, 'FinderProcess', 'FinderProcess: refreshed new nas items', nowait=True)
            if time.time() >= update_hke:
                self.update_hkse_list(cursor=cursor, connection=connection, msg_queue=msg_queue)
                update_hke = time.time() + self._refresh_hke
                self.msg(msg_queue, 'FinderProcess', 'FinderProcess: refreshed new hke items', nowait=True)

            if time.time() >= update_refresh:
                ticker_list = [('NXT.ax',), ('GOOG',)]
                ticker_list = ticker_list + self.query("SELECT id FROM unanalysed_tickers ORDER BY marketcap DESC", cursor=cursor)
                ticker_list = ticker_list + self.update_stock_refresh_list(cursor=cursor)
                # ticker_list = list(reversed(ticker_list))
                update_refresh = time.time() + (5 * 60)
                prefetched_tasks.set(len(ticker_list))
                self.msg(msg_queue, 'FinderProcess', 'FinderProcess: refreshed new tickers list', nowait=True)

            if time.time() >= last_update + self._max_query_per_sec:
                if len(ticker_list) > 0 and ticker_queue.qsize() <= self._max_idle_ticker_queue_size:
                    ticker_queue.put(ticker_list[0][0])
                    #self.msg(msg_queue, 'FinderProcess', 'Added {} to the listings'.format(ticker_list[0][0]))
                    ticker_list.pop(0)
                    prefetched_tasks.set(len(ticker_list))  # update prefetched item count
                    last_update = time.time()

            time.sleep(0.001)

    def collector(self, thread_exit_flag: bool, ticker_queue, data_out_queue, task_counters, msg_queue):
        self.msg(msg_queue, 'StartingCollectorThread', '\t\tdone.. collector thread started', style='system', nowait=True)

        queued_tasks, prefetched_tasks = task_counters

        # create string to describe next ticker loop
        ticker_opening_string = 'beginning new update: ' \
                                'ticker={s_format}{s_ticker}{s_end_format} ' \
                                '[{s_item_num}/{s_item_count} items]'
        ticker_opening_string = ticker_opening_string.format(s_format=bcolors.UNDERLINE + bcolors.BOLD + bcolors.HEADER,
                                                             s_ticker='{}',
                                                             s_end_format=bcolors.ENDC,
                                                             s_item_num='{}',
                                                             s_item_count='{}')

        item = 1
        while True:
            if thread_exit_flag.value():
                self.msg(msg_queue, 'CollectorThreadShutdown', '\t\tDone.. collector thread stopped', style='system', nowait=True)
                return

            # get next item
            try:
                ticker = ticker_queue.get(timeout=0.5)
                self.msg(msg_queue, 'CollectorGet', 'collecting next ticker "{}"'.format(ticker), nowait=True)
            except queue.Empty:
                continue

            msg_list = [(ticker, '\tAttempting collect info')]

            # setup required variables
            stock = None
            info = None
            cashflow = None

            try:
                stock = self.get_yahoo_finance_data(ticker)
                info = stock.info  # dict
                cashflow = stock.cash_flow  # _pd.DataFrame

                msg_list.append((ticker, '\t\tCollect Info Successful'))

                # package up data to send to evaluator
                data_package = (ticker, info, stock, cashflow)

                # output data
                data_out_queue.put(data_package)
            except TypeError as e:
                self.err_print(function='collector', msg='Failed to get cashflow', error=e)
                msg_list.append(
                    (ticker, '\t\tCollect Info {}Failed{}'.format(bcolors.FAIL, bcolors.ENDC)))
                msg_list.append((ticker, Queue.MsgDone))
            except Exception as e:
                if e.__str__() == 'yfinance failed to decrypt Yahoo data response':
                    #self.msg(msg_queue, 'Error', 'unable to get cashflow', nowait=True)
                    self.err_print(function='collector', msg='YFinance Failure: yfinance failed to decrypt Yahoo data response', error=e)

                    data_out_queue.put((ticker, info, stock, cashflow))
                else:
                    print(e)
                    self.msg(msg_queue, 'UnexpectedError', 'unexpected error received, exiting softly', nowait=True)
                    self.flags['e_stop_flag'].set(True)
                    return

            item_count = queued_tasks.read_increment()
            prefetched_items = prefetched_tasks.read()
            self.msg(msg_queue, ticker, ticker_opening_string.format(ticker, item_count, item_count + ticker_queue.qsize() + prefetched_items))

            for message in msg_list:
                self.msg(msg_queue, *message)

            # list current ticker queue item as done
            ticker_queue.task_done()
            item = item + 1

    def process_data(self, thread_exit_flag: bool, data_queue, msg_queue):
        self.msg(msg_queue, 'StartingProcessorThread', '\t\tdone.. processor thread started', style='system', nowait=True)

        connection, cursor = self.database_connect()

        while True:
            # get next data entry
            if thread_exit_flag.value():
                self.msg(msg_queue, 'ProcessorThreadShutdown', '\t\tDone.. processor thread stopped', style='system', nowait=True)
                return

            try:
                data = data_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            ticker_failed = False

            # else continue standard procedure. Extract expected data
            ticker, info, stock, cashflow = data

            try:
                # filter and consolidate cashflow info into clean useable data
                cashflow_statement = self.process_cashflow_data(ticker, cashflow, msg_queue=msg_queue)

                # write each annual cashflow statement to database [optimisation available to write all at once]
                for date in cashflow_statement:
                    annual_cf = cashflow_statement[date]
                    self.write_cashflow(ticker, date.year, *annual_cf, msg_queue, cursor=cursor,
                                        connection=connection)

                # next, evaluate results and determine new intrinsic value
                self.evaluate_intrinsic_value(ticker, msg_queue, connection=connection, cursor=cursor)

            except FailedTickerAnalysis as e:
                self.msg(msg_queue, ticker, '\t[Warning] Received FailedTickerAnalysis exception')
                ticker_failed = e.stock_anaylsis_failure
            finally:
                # stock now successfully updated
                self.update_stock_status(ticker=ticker, stock_info=info, stock=stock, failed=ticker_failed,
                                         msg_queue=msg_queue, connection=connection, cursor=cursor)

                # finally pass ticker back to finder function to mark off as complete
                self.remove_unanalysed_stock(ticker, msg_queue, cursor=cursor)

                self.msg(msg_queue, ticker, Queue.MsgDone)
                # mark current data queue item as done
                data_queue.task_done()

    def logger(self, thread_exit_flag: bool, message_queue):
        self.msg(message_queue, 'StartingLoggerThread', '\t\tdone.. logger thread started', nowait=True, style='system')

        message_packages = {}

        # fetching ticker
        #    finding info
        #    finding cashflow
        #    finding history
        #    ---------------
        #    building cashflow_statement
        #    writing cashflow data to sql database
        #    ---------------
        #    updating ticker status: success
        #    ---------------
        #    evaluating new intrinsic value
        #    writing intrinsic value to sql database
        #    ---------------
        #    removing ticker from todo list
        # .. Message.Done

        # build loading animation bars for longer time components
        # track current queue sizes for ticker and evaluator queues

        while True:
            # exit if end of queue
            if thread_exit_flag.value():
                self.print('logger thread stopped', style='system')
                return

            try:
                message_item = message_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            # unpack message information
            ticker, msg_time, message, style = message_item
            # if message is complete, print ticker message trail
            if message == Queue.MsgDone:
                self.print('', no_format=True)
                for line in message_packages[ticker]:
                    self.print('{}'.format(line[1]))

                # remove the no longer required message trail
                message_packages.pop(ticker)
            else:
                # add message to ticker msg trail
                if ticker in message_packages:
                    message_packages[ticker].append((msg_time, message))
                else:  # add new message package if this is first message
                    message_packages[ticker] = [(msg_time, message)]

            # list current task as finished
            message_queue.task_done()

    def query(self, query, cursor):
        # query all data from cashflow
        cursor.execute(query)

        # return results
        return cursor.fetchall()

    def query_write(self, query, connection, cursor, *args):
        try:
            # query all data from cashflow
            cursor.execute(query, *args)

            # commit changes
            connection.commit()

            # return results
            return cursor.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print('\t[query_write ERR] error updating ticker table')
            print('\t\tquery:', query)
            print('\t\targs:', args)
            print('\t\t', error)
            exit()
            connection.reset()

    def get_yahoo_finance_data(self, ticker):
        while time.time() < self._next_valid_query:
            time.sleep(0.01)

        # set next valid query time
        self._next_valid_query = time.time() + self._max_query_per_sec

        try:
            yfin_result = yfinance.Ticker(ticker)
        except (Exception,) as e:
            print('[yfinance ERR] ticker: {0} '.format(ticker))
            print(e)
            return None

        return yfin_result

    def write_cashflow(self, ticker, year, operating_cashflow, cap_expenditure, free_cashflow, msg_queue, cursor,
                       connection):
        # encapsulate inputs in ___
        cashflow_data = (ticker, year, operating_cashflow, cap_expenditure, free_cashflow)

        # sql to write data
        sql = """INSERT INTO cashflow(id, year, operating_cashflow, cap_expenditure, freecashflow)
                 VALUES(%s, %s, %s, %s, %s) returning id, year;"""

        try:
            # run query
            cursor.execute(sql, cashflow_data)

            # get first result of query
            rs = cursor.fetchall()

            # commit changes
            connection.commit()
        except psycopg2.errors.UniqueViolation as e:
            msg = 'Key (id, year)=({0}, {1}) already exists.'.format(ticker, year)
            self.err_print(function='write_cashflow', msg=msg, error=e, ticker=ticker, msg_queue=msg_queue, msg_only=True)
            connection.reset()
        except (Exception, psycopg2.DatabaseError) as e:
            print('\tERR: cashflow write error for {0}'.format(ticker))
            print('\tinputs were (tick, year, opcash, capex, FCF)=({}, {}, {}, {}, {})'.format(ticker, year,
                                                                                               operating_cashflow,
                                                                                               cap_expenditure,
                                                                                               free_cashflow))
            print(e)
            exit()
            self.connection.reset()

    def update_asx_list(self, cursor, connection):
        # sql to write data [optimisable if pre-check done in sql]
        sql = """INSERT INTO unanalysed_tickers(id, marketcap) VALUES %s RETURNING id;"""

        try:
            # get list of tickers
            results = pyasx.data.companies.get_listed_companies()
        except (Exception,) as e:
            print('l293: {}'.format(e))
            exit()

        print('generating stock list..')
        data = []
        for stock in results:
            stock_list = self.query("SELECT id FROM ticker WHERE id='{0}' "
                                    "UNION SELECT id FROM unanalysed_tickers WHERE id='{0}' "
                                    "LIMIT 1;".format(stock['ticker'] + '.ax'), cursor=cursor)
            if len(stock_list) == 0 and stock['market_cap'].isnumeric():
                data.append((stock['ticker'] + '.ax', int(int(stock['market_cap']) / 1000000)))

        if len(data) == 0:
            print('no new asx listings')
            return

        print('\tUpdating unanalysed stock list: adding {} new tickers'.format(len(data)))

        try:
            # run query
            psycopg2.extras.execute_values(cursor, sql, data, template=None, page_size=10000)

            # get first result of query
            rs = cursor.fetchall()

            print('added {0} new asx listings'.format(len(rs)))

            # commit changes
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as e:
            self.err_print(function=None, msg='Failed to write to database', error=e)
            exit()
            connection.reset()

    def update_nasdaq_list(self, cursor, connection, msg_queue):
        # sql to write data [optimisable if pre-check done in sql]
        sql = """INSERT INTO unanalysed_tickers(id, marketcap) VALUES %s RETURNING id;"""

        self.msg(msg_queue, 'scanning nasdaq', 'scanning nasdaq for new stocks')

        # download files
        if os.path.exists("nasdaq.lst"):
            os.remove("nasdaq.lst")
        if os.path.exists("otherlisted.lst"):
            os.remove("otherlisted.lst")

        self.msg(msg_queue, 'scanning nasdaq', '\tdownloading all nasdaq stocks')
        os.system("curl --ftp-ssl anonymous:jupi@jupi.com "
                  "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt "
                  "> nasdaq.lst")

        os.system("curl --ftp-ssl anonymous:jupi@jupi.com "
                  "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt "
                  "> otherlisted.lst")

        self.msg(msg_queue, 'scanning nasdaq', '\tloading data and columns')
        # load data
        try:
            # nasdaq
            colnames = ['ticker', 'name', 'category', 'TestIssue', 'status', 'size', 'IsEtf', 'NextShares']
            nasdaq = pd.read_csv("nasdaq.lst", sep='|', header=None, engine='python', names=colnames)
            # unlisted
            colnames = ['ticker', 'name', 'Exchange', 'CQSsymbol', 'IsEtf', 'RoundLotSize', 'TestIssue', 'NASSymbol']
            otherlisted = pd.read_csv("otherlisted.lst", sep='|', header=None, engine='python', names=colnames)
        except (Exception,) as e:
            self.err_print(function=None, msg='failed to download / read nasdaq files. returning',
                           error=e)

            self.msg(msg_queue, 'scanning nasdaq', Queue.MsgDone)
            exit()
            return

        self.msg(msg_queue, 'scanning nasdaq', '\tcleaning data')
        # trim header and footer rows
        nasdaq.drop(index=nasdaq.index[0], axis=0, inplace=True)
        nasdaq.drop(index=nasdaq.index[-1], axis=0, inplace=True)

        # remove non-standard stocks
        nasdaq.drop(nasdaq[nasdaq['NextShares'] != 'N'].index, axis=0, inplace=True)
        nasdaq.drop(nasdaq[nasdaq['IsEtf'] != 'N'].index, axis=0, inplace=True)
        nasdaq.drop(nasdaq[nasdaq['TestIssue'] != 'N'].index, axis=0, inplace=True)

        # reset index
        nasdaq.reset_index(inplace=True)

        # trim header and footer rows
        otherlisted.drop(index=otherlisted.index[0], axis=0, inplace=True)
        otherlisted.drop(index=otherlisted.index[-1], axis=0, inplace=True)

        # remove non-standard stocks
        otherlisted.drop(otherlisted[otherlisted['Exchange'] != 'N'].index, axis=0, inplace=True)
        otherlisted.drop(otherlisted[otherlisted['IsEtf'] != 'N'].index, axis=0, inplace=True)
        otherlisted.drop(otherlisted[otherlisted['TestIssue'] != 'N'].index, axis=0, inplace=True)
        otherlisted.drop(otherlisted[otherlisted['ticker'] != otherlisted['CQSsymbol']].index, axis=0, inplace=True)
        otherlisted.drop(otherlisted[otherlisted['ticker'] != otherlisted['NASSymbol']].index, axis=0, inplace=True)

        # reset index
        otherlisted.reset_index(inplace=True)

        self.msg(msg_queue, 'scanning nasdaq', '\tchecking if stocks already analysed')
        # read data into sql list
        data = []

        for ticker in nasdaq['ticker']:
            stock_list = self.query("SELECT id FROM ticker WHERE LOWER(id)=LOWER('{0}') "
                                    "UNION SELECT id FROM unanalysed_tickers WHERE LOWER(id)=LOWER('{0}') "
                                    "LIMIT 1;".format(ticker), cursor=cursor)
            if len(stock_list) == 0:
                data.append((ticker, -1))

        for ticker in otherlisted['ticker']:
            stock_list = self.query("SELECT id FROM ticker WHERE LOWER(id)=LOWER('{0}') "
                                    "UNION SELECT id FROM unanalysed_tickers WHERE LOWER(id)=LOWER('{0}') "
                                    "LIMIT 1;".format(ticker), cursor=cursor)
            if len(stock_list) == 0:
                data.append((ticker, -1))

        if len(data) == 0:
            self.msg(msg_queue, 'scanning nasdaq', '\tno new stocks found, exiting with no actions')
            self.msg(msg_queue, 'scanning nasdaq', Queue.MsgDone)
            return

        self.msg(msg_queue, 'scanning nasdaq', '\twriting new NASDAQ stocks to sql database')
        try:
            # run query
            psycopg2.extras.execute_values(cursor, sql, data, template=None, page_size=10000)

            # get first result of query
            rs = cursor.fetchall()

            # commit changes
            connection.commit()
            self.msg(msg_queue, 'scanning nasdaq', 'added {0} new nasdaq tickers'.format(len(rs)))
            self.msg(msg_queue, 'scanning nasdaq', Queue.MsgDone)
        except psycopg2.errors.UniqueViolation as e:
            connection.reset()
        except (Exception, psycopg2.DatabaseError) as e:
            self.err_print(function=None, msg='Failed to write to database', error=e)

            self.msg(msg_queue, 'scanning nasdaq', Queue.MsgDone)
            exit()
            connection.reset()

    def update_hkse_list(self, cursor, connection, msg_queue):
        sql = """INSERT INTO unanalysed_tickers(id, marketcap) VALUES %s RETURNING id;"""

        # build list from wiki
        # 'https://en.wikipedia.org/wiki/List_of_companies_listed_on_the_Hong_Kong_Stock_Exchange'

        self.msg(msg_queue, 'scanning hkse', 'scanning hkse for new stocks')
        self.msg(msg_queue, 'scanning hkse', '\tdownloading html')
        # get the page Details
        page = wikipedia.page("List of companies listed on the Hong Kong Stock Exchange")
        content = page.html()

        soup = BeautifulSoup(content, "html.parser")
        # for a in soup.findAll('a', href=True, attrs={'class': '_31qSD5'}):
        #    name = a.find('div', attrs={'class': '_3wU53n'})

        self.msg(msg_queue, 'scanning hkse', '\tscanning html for table items')
        data = []
        for table in soup.findAll('table'):
            for a in table.findAll('a', href=True, attrs={'class': 'external text'}):
                if a.text.isnumeric():
                    ticker = '{:04d}'.format(int(a.text)) + '.HK'

                    stock_list = self.query("SELECT id FROM ticker WHERE id='{0}' "
                                            "UNION SELECT id FROM unanalysed_tickers WHERE id='{0}' "
                                            "LIMIT 1;".format(ticker), cursor=cursor)
                    if len(stock_list) == 0:
                        data.append((ticker, -1))

        if len(data) == 0:
            self.msg(msg_queue, 'scanning hkse', '\tno new stocks found, exiting with no actions')
            return

        self.msg(msg_queue, 'scanning hkse', '\tfound {} new listings'.format(len(data)))

        try:
            # run query
            psycopg2.extras.execute_values(cursor, sql, data, template=None, page_size=10000)

            # get first result of query
            rs = cursor.fetchall()

            print('added {0} new hkse listings'.format(len(rs)))

            # commit changes
            connection.commit()
            self.msg(msg_queue, 'scanning hkse', '\tnew listings successfully added')
            self.msg(msg_queue, 'scanning hkse', Queue.MsgDone)
        except (Exception, psycopg2.DatabaseError) as e:
            self.err_print(function=None, msg='Failed to write to database', error=e)
            self.msg(msg_queue, 'scanning hkse', '\terror adding listings to sql database')
            self.msg(msg_queue, 'scanning hkse', Queue.MsgDone)
            connection.reset()

    def update_stock_refresh_list(self, cursor):
        base_sql = """SELECT id FROM ticker WHERE (CURRENT_DATE-last_update) > 14 
                        OR (update_attempts>0 AND update_attempts<10 AND CURRENT_DATE-last_update>31) 
                        ORDER BY marketcap DESC"""

        base_sql = """SELECT id FROM ticker WHERE {conditions} ORDER BY marketcap DESC"""

        # define time conditions
        time_condition = 'CURRENT_DATE-last_update > {period}'
        time_condition1 = time_condition.format(period=self.standard_update_period)
        time_condition2 = time_condition.format(period=self.failed_retry_update_period)
        time_condition3 = time_condition.format(period=self.ultimate_failed_update_period)

        condition1 = '(update_attempts = 0 AND {time_condition})'.format(time_condition=time_condition1)
        condition2 = '(update_attempts > 0 AND update_attempts < {attempts} AND {time_condition})'.format(
            attempts=self.retry_attempts, time_condition=time_condition2)
        condition3 = '(update_attempts > {attempts} AND {time_condition})'.format(attempts=self.retry_attempts,
                                                                                  time_condition=time_condition3)

        # combine conditions
        conditions = '{0} OR {1} OR {2}'.format(condition1, condition2, condition3)

        # add conditional constraints to sql query
        sql = base_sql.format(conditions=conditions)

        return self.query(sql, cursor=cursor)

    def remove_unanalysed_stock(self, ticker, msg_queue, cursor):
        sql = "DELETE FROM unanalysed_tickers WHERE id='{0}' RETURNING id;"

        self.msg(msg_queue, ticker, '\tremoving {0} from unanalysed stocks'.format(ticker))

        result = self.query(sql.format(ticker), cursor=cursor)
        if len(result) > 0:
            self.msg(msg_queue, ticker, '\t\tsuccessfully removed item from sql table')
        else:
            self.msg(msg_queue, ticker, '\t\tfailed to remove item from sql table')

    def get_cashflow(self, ticker, cursor, msg_queue):
        sql = "SELECT year, operating_cashflow, cap_expenditure, freecashflow FROM cashflow WHERE id='{0}' ORDER BY year;"
        cashflow = self.query(sql.format(ticker), cursor=cursor)

        # format into a dataframe
        formatted_cashflow = pd.DataFrame(cashflow, columns=['year', 'op_cashflow', 'capex', 'fcf']).set_index('year')
        formatted_cashflow['capex'] = formatted_cashflow['capex'] * -1

        # safeguard against stocks with only 1 year of data
        if len(formatted_cashflow) < 2:
            self.msg(msg_queue, ticker, '\tInsufficient data to calculate intrinsic value - found {} years of data'.format(len(formatted_cashflow)))
            raise FailedTickerAnalysis(stock_anaylsis_failure=False)

        # if data contains negative numbers, left adjust to avoid expected results
        op_cash_np = formatted_cashflow['op_cashflow'].to_numpy()
        if min(op_cash_np) <= 0:
            if all(i <= 0 for i in op_cash_np):
                small = max(op_cash_np[op_cash_np < 0]) * -1
            else:
                small = min(number for number in op_cash_np if number > 0)
            formatted_cashflow['op_cashflow'] = op_cash_np - min(op_cash_np) + small

        capex_np = formatted_cashflow['capex'].to_numpy()
        if min(capex_np) <= 0:
            if all(i <= 0 for i in capex_np):
                small = max(capex_np) * -1
            else:
                small = min(number for number in capex_np if number > 0)
            formatted_cashflow['capex'] = capex_np - min(capex_np) + small

        return formatted_cashflow

    def get_stock_info(self, ticker, stock_info, stock, cursor, msg_queue):
        if stock_info is not None:
            # get stock name
            name = stock_info.get('longName', 'NA') or 'NA'

            # get stock sector
            sector = stock_info.get('sector', 'NA')
            # get current stock market price
            try:
                market_price = stock.history(period='1d', )['Close'].iloc[-1]
            except Exception as e:
                msg = 'Failed to get market price data from stock.history'
                self.err_print(function='get_stock_info', msg=msg, error=e, ticker=ticker, msg_queue=msg_queue,
                               msg_only=False)
                market_price = -1

            # get number of outstanding shares
            shares_outstanding = stock_info.get('sharesOutstanding', -1)
            if shares_outstanding is None: shares_outstanding = -1

            # calculate market capitalisation using cap = shares*price
            if shares_outstanding > 0:
                market_cap = int(shares_outstanding * market_price / 1000000)
                stock_shares_outstanding = int(round(shares_outstanding / 1000))
            else:
                market_cap = -1
        else:
            name = 'NA'
            sector = 'NA'
            shares_outstanding = -1
            market_cap = -1

        # query update attempts for ticker (to determine existance in table and failure count if applicable)
        failure_count = self.query("SELECT update_attempts FROM ticker WHERE id='{0}'".format(ticker), cursor=cursor)

        if len(failure_count) == 0:
            existing_in_sql = False
        else:
            existing_in_sql = True
            failure_count = failure_count[0][0]

        return name, sector, shares_outstanding, market_cap, existing_in_sql, failure_count

    def evaluate_intrinsic_value(self, ticker, msg_queue, connection, cursor) -> bool:
        # get all current data
        cashflow = self.get_cashflow(ticker, cursor, msg_queue=msg_queue)

        # First calculate G1
        g1_growth = self.calculate_g1_growth(cashflow)

        # Second calculate G2
        g2_growth = self.calculate_g2_growth(cashflow)

        # Third calculate G3
        g3_growth = self.calculate_g3_growth(cashflow)

        # concatenate growth rates into an array for ease of use
        growth_rates = [g1_growth, g2_growth, g3_growth]

        # estimate 10 year growth rate to be half the estimated growth rate
        growth_10yr = [([min(0.5 * g[0], 0.2), 0.5 * g[1]]) for g in growth_rates]

        # ensure no insane negative growth values
        for g in range(len(growth_rates)):
            for i in range(2):
                if growth_rates[g][i] < -0.99:
                    growth_rates[g][i] = -0.99

        # predicted growth
        predicted_earnings = [cashflow['op_cashflow'].iloc[-1]] * len(growth_rates)
        predicted_capex = [cashflow['capex'].iloc[-1]] * len(growth_rates)

        # create buffer array to store intrinsic value estimates
        intrinsic = [0] * len(growth_rates)

        # for each growth estimate calculate the predicated free cash flows
        for g in range(len(growth_rates)):
            for year_dx in range(10):
                # predict likely earnings
                predicted_earnings[g] = predicted_earnings[g] * (
                        1 + growth_rates[g][0] - ((growth_rates[g][0] - growth_10yr[g][0]) / 9) * year_dx)

                predicted_capex[g] = predicted_capex[g] * (
                        1 + growth_rates[g][1] - ((growth_rates[g][1] - growth_10yr[g][1]) / 9) * year_dx)
                # assume capex can never be negative (aka, your debt will never make you money, best debt = no debt)
                if predicted_capex[g] < 0:
                    predicted_capex[g] = 0

                predicted_fcf = predicted_earnings[g] - predicted_capex[g]
                # update discount factor
                discount_factor = pow(1 + _discount_rate, year_dx + 1)

                # discounted future values
                discounted_pred_fcf = predicted_fcf / discount_factor

                # add to intrinsic value calc
                intrinsic[g] += discounted_pred_fcf

            # calculate Terminal value (selling price of company) - alternatively consider a market multiplier
            terminal_fcf = predicted_fcf * (1 + _growth_perpetuity) / (_discount_rate - _growth_perpetuity)
            discounted_terminal_fcf = terminal_fcf / discount_factor

            # update intrinsic value with selling price
            intrinsic[g] += discounted_terminal_fcf

            # convert intrinsic values to int for input to sql database
            intrinsic[g] = int(intrinsic[g])

        # ---- Intrinsic value calculated, prep for database sql write ----
        # format data to database table specifications
        intrinsic = np.array(intrinsic)  # numpy array
        intrinsic = intrinsic / 1000  # div by 1000 to save memory
        intrinsic[intrinsic > 2147483647] = 2147483647  # ensure size within INT32 limits
        intrinsic[intrinsic < 0] = 0.0  # ensure no negative values
        intrinsic = [round(i) for i in intrinsic]  # round all values to nearest integer

        # log output
        intrinsic_value_desc = '\tintrinsic value estimate calculated: {col}{value}{e_col}   ({timeframe} years of data)'.format(
            value=millify(intrinsic[1] * 1000000), col=bcolors.OKGREEN, e_col=bcolors.ENDC, timeframe=len(cashflow))
        self.msg(msg_queue, ticker, intrinsic_value_desc)

        # log asset growth
        if growth_rates[1][0] > 0:
            col = bcolors.OKGREEN
        else:
            col = bcolors.FAIL
        rev_growth_desc = '\toperating activities revenue growth: {}{:+.1f}%{}'.format(
            col, 100 * growth_rates[1][0], bcolors.ENDC)
        self.msg(msg_queue, ticker, rev_growth_desc)

        # log cost of asset growth
        if growth_rates[1][0] > growth_rates[1][1]:
            col = bcolors.OKGREEN
        else:
            col = bcolors.FAIL
        costofrev_growth_desc = '\tcost of operating activities growth: {}{:+.1f}%{}'.format(
            col, 100 * growth_rates[1][1], bcolors.ENDC)
        self.msg(msg_queue, ticker, costofrev_growth_desc)

        # prep sql statements
        # check if intrinsic value table has ticker
        table_id = self.query("SELECT id FROM intrinsicvalue WHERE id='{0}'".format(ticker), cursor=cursor)

        # sql for ticker table
        sql_1 = """UPDATE ticker SET intrinsicvalue_min=%s, intrinsicvalue_best=%s WHERE id=%s RETURNING id;"""
        values_1 = (min(intrinsic), intrinsic[1], ticker)

        # sql for intrinsic value table
        if len(table_id) == 0:  # if not in table, add it
            sql_2 = """INSERT INTO intrinsicvalue(id, growth_rate_1a, growth_rate_1b, growth_rate_2a, 
                    growth_rate_2b, growth_rate_3a, growth_rate_3b, intrinsicvalue_1, intrinsicvalue_2, 
                    intrinsicvalue_3) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning id;"""

            values_2 = (ticker, *g1_growth, *g2_growth, *g3_growth, *intrinsic)
        else:  # if already in table, update it
            sql_2 = """UPDATE intrinsicvalue SET growth_rate_1a=%s, growth_rate_1b=%s, growth_rate_2a=%s, 
                    growth_rate_2b=%s, growth_rate_3a=%s, growth_rate_3b=%s, intrinsicvalue_1=%s, 
                    intrinsicvalue_2=%s, intrinsicvalue_3=%s WHERE id=%s RETURNING id;"""
            values_2 = (*g1_growth, *g2_growth, *g3_growth, *intrinsic, ticker)

        # update ticker table and intrinsic value table
        results_1 = self.query_write(sql_1, connection, cursor, values_1)
        results_2 = self.query_write(sql_2, connection, cursor, values_2)

        if len(results_2) > 0:
            self.msg(msg_queue, ticker, '\tsuccessfully updated intrinsic value evaluation')
        else:
            self.msg(msg_queue, ticker, '\t{}failed{} to update intrinsic value evaluation'.format(bcolors.FAIL, bcolors.ENDC))

    def update_stock_status(self, ticker, stock_info, stock, failed, msg_queue, connection, cursor, comment=''):
        name, sector, shares_outstanding, market_cap, existing_in_sql, failure_count = self.get_stock_info(ticker,
                                                                                                           stock_info,
                                                                                                           stock,
                                                                                                           cursor=cursor,
                                                                                                           msg_queue=msg_queue)

        # determine value for update_attempts
        if not failed:
            failure_count = 0
            update_status_str = bcolors.OKGREEN + '\tSuccessfully updated indices {ticker}'.format(
                ticker=ticker) + bcolors.ENDC
        elif existing_in_sql:
            failure_count = failure_count + 1
            update_status_str = bcolors.FAIL + '\tupdating attempt failure ({0} -> {1})'.format(failure_count - 1,
                                                                                                failure_count) + bcolors.ENDC
        else:
            failure_count = 1
            update_status_str = bcolors.FAIL + '\tupdating attempt failure ({0} -> {1})'.format(0, 1) + bcolors.ENDC

        self.msg(msg_queue, ticker, update_status_str)

        # if ticker doesn't exist in table, add it, else update it
        if not existing_in_sql:
            sql = """INSERT INTO ticker(id, name, sector, marketcap, shares_outstanding, last_update, update_attempts, comment) VALUES(%s, %s, %s, %s, %s, CURRENT_DATE, %s, %s) returning id;"""
            values = (ticker, name, sector, market_cap, shares_outstanding, failure_count, comment)
        elif not failed:
            sql = """UPDATE ticker SET marketcap=%s, shares_outstanding=%s, last_update=CURRENT_DATE, update_attempts=%s WHERE id=%s RETURNING id;"""
            values = (market_cap, shares_outstanding, failure_count, ticker)
        else:
            sql = """UPDATE ticker SET last_update=CURRENT_DATE, update_attempts=%s, comment=%s WHERE id=%s RETURNING id;"""
            values = (failure_count, comment, ticker)

        result = self.query_write(sql, connection, cursor, values)

    def process_cashflow_data(self, ticker: str, cashflow: pd.DataFrame, msg_queue) -> pd.DataFrame:
        if cashflow is None or cashflow.empty:
            msg = '\tUpdate ERR: {0} cashflow statement empty, ignore and continue'.format(ticker)
            self.err_print(function='process_cashflow_data', msg=msg, error=None, ticker=ticker, msg_queue=msg_queue,
                           msg_only=True)
            raise FailedTickerAnalysis

        # find operating cashflow
        if 'Operating Cash Flow' in cashflow.index:
            df_opcashflow = cashflow.loc['Operating Cash Flow']
        elif 'Cash Flowsfromusedin Operating Activities Direct' in cashflow.index:
            df_opcashflow = cashflow.loc['Cash Flowsfromusedin Operating Activities Direct']
        else:
            print('ERROR: no operating cash flow found!')
            exit()

        # find freecashflow (FCF)
        if 'Free Cash Flow' in cashflow.index:
            df_fcf = cashflow.loc['Free Cash Flow']
        else:
            print('ERROR: no free cash flow found!')
            exit()

        # find or calculate capital expenditure
        if 'Capital Expenditure' in cashflow.index:
            df_capex = cashflow.loc['Capital Expenditure']  # cols.pop(1)
            if pd.DataFrame(df_capex).isnull().values.any():
                df_capex = -1 * (df_opcashflow - df_fcf)
        else:
            df_capex = -1 * (df_opcashflow - df_fcf)

        if (df_capex > 0).any():
            print('\tERR: positive CapEx data found for {}, setting stock as failed'.format(ticker))
            raise FailedTickerAnalysis

        # combine desired columns and transpose back to year as index
        cashflow_statements = pd.concat([df_opcashflow, df_capex, df_fcf], axis=1).transpose()

        # if any null values, return bad data
        if cashflow_statements.isnull().values.any():
            print('\tERR: null values detected in cashflow data')
            raise FailedTickerAnalysis

        # divide everything by 1000 (we dont care about that level of precision)
        cashflow_statements = cashflow_statements / 1000

        # if the company's numbers are larger than max integer size, then don't write values to database
        if (cashflow_statements.abs() > 2147483647).any().any():
            print('Revenue above integer capacity for ticker: {}'.format(ticker))
            raise FailedTickerAnalysis

        return cashflow_statements

    def print_time_remaining(self, timeout, msg_queue):
        # calculate time remaining
        remaining_time = self.time_start + timeout - time.time()

        # calculate minutes and seconds
        minutes = math.floor(remaining_time / 60)
        seconds = math.floor(remaining_time % 60)

        # print correctly formatted time results. >60s = m:ss. <60 = ss
        if minutes:
            self.msg(msg_queue, 'StopTime', 'stopping in {:.0f}:{:02.0f}s'.format(minutes, seconds), style='system', nowait=True)
        else:
            self.msg(msg_queue, 'StopTime', 'stopping in {:.0f}s'.format(seconds), style='system', nowait=True)

    def print(self, msg, style='default', no_format=False):
        style_dict = {'system': bcolors.SYSTEM}

        if style in style_dict.keys():
            msg = '{}{}{}'.format(style_dict[style], msg, bcolors.ENDC)
        self.print_handle.print(msg, no_format)

        return True

    def err_print(self, function='No Func Provided', msg='', error=None, ticker=None, msg_queue=None, msg_only=False):

        string_err = '\t{s_format}[ERR {s_func}] {s_msg}{e_format}'

        if not msg_only:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            function = exc_tb.tb_frame.f_code.co_name
            line_num = exc_tb.tb_lineno

            error_desc = '{type} line {line}: '.format(type=re.split("'", str(exc_type))[1], line=line_num)

            error_desc_components = re.split('\n', error.__str__().strip())
            if len(error_desc_components) > 1:
                error_desc = error_desc + error_desc_components[0]
            for line in error_desc_components[1:]:
                error_desc = error_desc + '\n' + '\t' * 6 + line.strip('\n')

            if not msg_only:
                string_err = string_err + '\n' + '\t' * 6 + '{s_err}'.format(s_err=error_desc)

        string_err = string_err.format(s_func=function, s_msg=msg, s_format=bcolors.WARNING, e_format=bcolors.ENDC)

        if msg_queue is None:
            print(string_err)
        else:
            self.msg(msg_queue, ticker, string_err, style='fail')
        # print(exc_type, fname, exc_tb.tb_lineno)

    @staticmethod
    def msg(msg_queue, topic, msg, style='default', nowait=False):
        style_dict = {'system': bcolors.SYSTEM, 'warning': bcolors.WARNING, 'fail': bcolors.FAIL}

        if style in style_dict.keys():
            msg = '{}{}{}'.format(style_dict[style], msg, bcolors.ENDC)

        payload = (topic, dt.now(), msg, style)
        msg_queue.put(payload)

        if nowait:
            payload = (topic, dt.now(), Queue.MsgDone, style)
            msg_queue.put(payload)

    @staticmethod
    def calculate_g1_growth(cashflow):
        # calc g1 growth
        op_cashflow_growth = []
        cap_ex_growth = []
        for i in range(len(cashflow) - 1):
            op_cashflow_growth.append(cashflow['op_cashflow'].iloc[i + 1] / cashflow['op_cashflow'].iloc[i] - 1)
            if cashflow['capex'].iloc[i] != 0:
                cap_ex_growth.append(cashflow['capex'].iloc[i + 1] / cashflow['capex'].iloc[i] - 1)
            else:
                cap_ex_growth.append(0)

        g1_growth = [sum(op_cashflow_growth) / len(op_cashflow_growth), sum(cap_ex_growth) / len(cap_ex_growth)]

        # if all capex values are identical dont calculate (just set value as 0)
        if (cashflow['capex'] == cashflow['capex'].iloc[0]).all():
            g1_growth[1] = 0

        return g1_growth

    @staticmethod
    def calculate_g2_growth(cashflow):
        g2_growth = [0, 0]
        g2_growth[0] = np.polyfit(cashflow['op_cashflow'].index.to_numpy(),
                                  np.log(cashflow['op_cashflow']), 1)[0]

        # if all capex values are identical dont calculate (just set value as 0)
        if not (cashflow['capex'] == cashflow['capex'].iloc[0]).all():
            g2_growth[1] = np.polyfit(cashflow['capex'].index.to_numpy(), np.log(cashflow['capex'].to_numpy()), 1)[0]
        else:
            g2_growth[1] = 0

        return g2_growth

    @staticmethod
    def calculate_g3_growth(cashflow):
        g3_growth = [0, 0]
        g3_growth[0] = np.polyfit(cashflow['op_cashflow'].index.to_numpy(),
                                  cashflow['op_cashflow'].to_numpy(), 1)[0] / np.average(cashflow['op_cashflow'])

        # if all capex values are identical dont calculate (just set value as 0)
        if not (cashflow['capex'] == cashflow['capex'].iloc[0]).all():
            g3_growth[1] = np.polyfit(cashflow['capex'].index.to_numpy(),
                                      cashflow['capex'].to_numpy(), 1)[0] / np.average(cashflow['capex'])
        else:
            g3_growth[1] = 0

        return g3_growth


def yfinance_func_test():
    results = []

    try:
        msft = yfinance.Ticker("MSFT")
        print('ticker {}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        results.append(True)
    except Exception as e:
        print('{}failed{} on ticker'.format(bcolors.FAIL, bcolors.ENDC))
        results.append(False)

    try:
        msft.info
        print('info {}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        results.append(True)
    except Exception as e:
        print('{}failed{} on info'.format(bcolors.FAIL, bcolors.ENDC))
        results.append(False)

    try:
        msft.fast_info
        print('fast info {}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        results.append(True)
    except Exception as e:
        print('{}failed{} on fast info'.format(bcolors.FAIL, bcolors.ENDC))
        results.append(False)

    try:
        hist = msft.cashflow
        print('cashflow {}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        results.append(True)
    except Exception as e:
        print('{}failed{} on cashflow'.format(bcolors.FAIL, bcolors.ENDC))
        results.append(False)

    try:
        hist = msft.history(period="1mo")
        print('history {}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        results.append(True)
    except Exception as e:
        print('{}failed{} on history'.format(bcolors.FAIL, bcolors.ENDC))
        results.append(False)

    return results


def yquery_func_test():
    results = []

    try:
        msft = yahooquery.Ticker("PME.ax")
        print('ticker {}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        results.append(True)
    except Exception as e:
        print('{}failed{} on ticker'.format(bcolors.FAIL, bcolors.ENDC))
        results.append(False)

    print(msft.asset_profile)
    #print(msft.summary_detail)
    #print(msft.cash_flow())
    exit()
    try:
        msft.info
        print('info {}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        results.append(True)
    except Exception as e:
        print('{}failed{} on info'.format(bcolors.FAIL, bcolors.ENDC))
        results.append(False)

    try:
        msft.fast_info
        print('fast info {}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        results.append(True)
    except Exception as e:
        print('{}failed{} on fast info'.format(bcolors.FAIL, bcolors.ENDC))
        results.append(False)

    try:
        hist = msft.cashflow
        print('cashflow {}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        results.append(True)
    except Exception as e:
        print('{}failed{} on cashflow'.format(bcolors.FAIL, bcolors.ENDC))
        results.append(False)

    try:
        hist = msft.history(period="1mo")
        print('history {}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        results.append(True)
    except Exception as e:
        print('{}failed{} on history'.format(bcolors.FAIL, bcolors.ENDC))
        results.append(False)

    return results


def main():

    #test_results = yfinance_func_test()
    #print('\nyfinance passed {}/{}'.format(test_results.count(True), len(test_results)))

    #test_results = yquery_func_test()
    #print('\nyfinance passed {}/{}'.format(test_results.count(True), len(test_results)))

    return
    # create database object
    pi_sql = Database()

    try:
        pi_sql.start(timeout=30*60, silent=False)
    except KeyboardInterrupt:
        pi_sql.stop()
    finally:
        print('Program finished')


if __name__ == '__main__':
    main()
