# generic imports
import time
import os
import sys
import pandas as pd
import numpy as np
import re
import math

# scrapping requests
import requests
import wikipedia
import yfinance
from yahooquery import Ticker # interesting option
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras
import pyasx.data.companies
from tqdm import tqdm


HourlyLimit = 480

_discount_rate = 0.15
_growth_perpetuity = 0.025

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
    millnames = ['','K','M','B','T']
    n = float(n)
    millidx = max(0,min(len(millnames)-1,
                        int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

    if millidx <= 4:
        suffix = millnames[millidx]
    else:
        suffix = ' * 10^{0}'.format(millidx*3)

    return '${:.2f}{}'.format(n / 10**(3 * millidx), suffix)


class Database:
    def __init__(self):
        database = "invest_simple"
        host = "139.130.195.51"  # "192.168.15.200"
        user = "pi"  # "interface_account"
        password = "actionN06"
        port = "5432"

        # define query limits
        self.max_query_per_sec = 60 / HourlyLimit
        self.next_valid_query = time.time() + self.max_query_per_sec

        # define constraint definitions for query period
        self.standard_update_period = 7             # re-poll period (days) for normal conditions
        self.failed_retry_update_period = 21        # re-poll period (days) if failed
        self.ultimate_failed_update_period = 186    # re-poll period (days) if failed too many times
        self.retry_attempts = 12                    # number of re-poll attempts before listing as low priority

        # attempt to connect to database
        try:
            # connect to database
            self.conn = psycopg2.connect(database=database,
                                         host=host,
                                         user=user,
                                         password=password,
                                         port=port)

            # get cursor
            self.cursor = self.conn.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            exit()
        # finally:
        #    if self.conn is not None:
        #        self.conn.close()

    def query(self, query):
        # query all data from cashflow
        self.cursor.execute(query)

        # return results
        return self.cursor.fetchall()

    def query_write(self, query, *args):
        try:
            # query all data from cashflow
            self.cursor.execute(query, *args)

            # commit changes
            self.conn.commit()

            # return results
            return self.cursor.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print('\t[query_write ERR] error updating ticker table')
            print(error)
            exit()
            self.conn.reset()

    def get_yahoo_finance_data(self, ticker):
        while time.time() < self.next_valid_query:
            time.sleep(0.01)

        # set next valid query time
        self.next_valid_query = time.time() + self.max_query_per_sec

        try:
            yfin_result = yfinance.Ticker(ticker)
        except (Exception,) as e:
            print('[yfinance ERR] ticker: {0} '.format(ticker))
            print(e)
            return None

        return yfin_result

    def get_yahoo_query_data(self, ticker):
        while time.time() < self.next_valid_query:
            time.sleep(0.01)

        # set next valid query time
        self.next_valid_query = time.time() + self.max_query_per_sec

        # run query
        try:
            yfin_result = Ticker(ticker)
        except (Exception,) as e:
            print('[yquery ERR] ticker: {0} '.format(ticker))
            print(e)
            return None

        return yfin_result

    def read_cashflow(self):
        # query all data from cashflow
        self.cursor.execute("SELECT * FROM cashflow ORDER BY id;")

        # return results
        return self.cursor.fetchall()

    def write_cashflow(self, ticker, year, operating_cashflow, cap_expenditure, free_cashflow):
        # encapsulate inputs in ___
        cashflow_data = (ticker, year, operating_cashflow, cap_expenditure, free_cashflow)

        # sql to write data
        sql = """INSERT INTO cashflow(id, year, operating_cashflow, cap_expenditure, freecashflow)
                 VALUES(%s, %s, %s, %s, %s) returning id, year;"""

        try:
            # run query
            self.cursor.execute(sql, cashflow_data)

            # get first result of query
            rs = self.cursor.fetchall()

            # commit changes
            self.conn.commit()
        except psycopg2.errors.UniqueViolation as e:
            msg = 'Key (id, year)=({0}, {1}) already exists.'.format(ticker, year)
            self.err_print(None, msg, e)
            self.conn.reset()
        except (Exception, psycopg2.DatabaseError) as e:
            print('\tERR: cashflow write error for {0}'.format(ticker))
            print('\tinputs were (tick, year, opcash, capex, FCF)=({}, {}, {}, {}, {})'.format(ticker, year,
                                                                                               operating_cashflow,
                                                                                               cap_expenditure,
                                                                                               free_cashflow))
            print(e)
            exit()
            self.conn.reset()

    def update_stock_list(self, ticker, stock_info=None, failed=False, comment=''):
        if stock_info is not None:
            try:
                stock_info = stock_info.get_info()
            except TypeError as e:
                self.err_print(function='update_stock_list', msg='Failed to get stock info', error=e)

                if str(e) == "string indices must be integers, not 'str'":
                    stock_info = None
                else:
                    print('[unknown error in update_stock_list], exiting gracefully')
                    exit()
            except Exception as e:
                yahoobug = 'Yahoo has again changed data format, yfinance now unsure which key(s) is for decryption'
                if str(e)[:87] == yahoobug:
                    self.err_print(None, 'Yahoo decryption bug', str(e)[:87])
                    stock_info = None
                    return False
                else:
                    raise

        if stock_info is not None:
            # get stock name
            stock_name = stock_info.get('longName', 'NA') or 'NA'

            # get stock sector
            stock_sector = stock_info.get('sector', 'NA')

            # get current stock market price
            stock_market_price = stock_info.get('regularMarketPreviousClose', 'NA')

            # get number of outstanding shares
            stock_shares_outstanding = stock_info.get('sharesOutstanding', -1)
            if stock_shares_outstanding is None: stock_shares_outstanding = -1

            # calculate market capitalisation using cap = shares*price
            if stock_shares_outstanding > 0:
                stock_market_cap = int(stock_shares_outstanding * stock_market_price / 1000000)
                stock_shares_outstanding = int(round(stock_shares_outstanding / 1000))
            else:
                stock_market_cap = -1
        else:
            stock_name = 'NA'
            stock_sector = 'NA'
            stock_shares_outstanding = -1
            stock_market_cap = -1

        # query update attempts for ticker (to determine existance in table and failure count if applicable)
        query_fail_count = self.query("SELECT update_attempts FROM ticker WHERE id='{0}'".format(ticker))

        # determine value for update_attempts
        if failed:
            if len(query_fail_count) == 0:
                update_attempts = 1
            else:
                update_attempts = query_fail_count[0][0] + 1
            print(bcolors.FAIL + '\tupdating attempt failure ({0} -> {1})'.format(update_attempts - 1, update_attempts) + bcolors.ENDC)
        else:
            update_attempts = 0
            print(bcolors.OKGREEN + '\tSuccessfully updated indices {ticker}'.format(ticker=ticker) + bcolors.ENDC)

        # if ticker doesn't exist in table, add it, else update it
        if len(query_fail_count) == 0:
            sql = """INSERT INTO ticker(id, name, sector, marketcap, shares_outstanding, last_update, update_attempts, comment) VALUES(%s, %s, %s, %s, %s, CURRENT_DATE, %s, %s) returning id;"""
            values = (
                ticker, stock_name, stock_sector, stock_market_cap, stock_shares_outstanding, update_attempts, comment)
            print('\t{0} not found in ticker table. Adding {0} to ticker table'.format(ticker))
        elif update_attempts == 0:
            sql = """UPDATE ticker SET marketcap=%s, shares_outstanding=%s, last_update=CURRENT_DATE, update_attempts=%s WHERE id=%s RETURNING id;"""
            values = (stock_market_cap, stock_shares_outstanding, update_attempts, ticker)
            print('\tupdating ticker table')
        else:
            sql = """UPDATE ticker SET last_update=CURRENT_DATE, update_attempts=%s, comment=%s WHERE id=%s RETURNING id;"""
            values = (update_attempts, ticker, comment)
            print('\tupdating ticker table')

        self.query_write(sql, values)

    def update_stock(self, ticker):
        # get stock info
        # stock = yfinance.Ticker(ticker)
        stock = self.get_yahoo_finance_data(ticker)

        if stock is None:
            print('FAILED')
            exit()

        # get cashflow
        try:
            stock_cashflow = stock.cashflow
        except TypeError as e:
            self.err_print(function='update_stock', msg='Failed to get cashflow', error=e)

            stock_yquery = self.get_yahoo_query_data(ticker)
            stock_yquery_cashflow = stock_yquery.cash_flow()

            if str(e) == "string indices must be integers, not 'str'" and type(stock_yquery_cashflow) is str and stock_yquery_cashflow[:26] == 'Cash Flow data unavailable':
                self.update_stock_list(ticker, stock_info=stock, failed=True)
                return False
            else:
                print('[unknown error in update_stock], exiting gracefully')
                exit()
        except Exception as e:
            yahoobug = 'Yahoo has again changed data format, yfinance now unsure which key(s) is for decryption'
            if str(e)[:87] == yahoobug:
                self.err_print(None, 'Yahoo decryption bug', str(e)[:87])
                self.update_stock_list(ticker, stock_info=stock, failed=True)
                return False
            else:
                raise

        if stock_cashflow.empty:
            print('\tUpdate ERR: {0} cashflow statement empty, ignore and continue'.format(ticker))
            self.update_stock_list(ticker, stock_info=stock, failed=True)
            return False

        # get years
        # cols = ['Operating Cash Flow', 'Capital Expenditure', 'Free Cash Flow']
        if 'Operating Cash Flow' in stock_cashflow.index:
            df_opcashflow = stock_cashflow.loc['Operating Cash Flow']
        #elif 'OperatingCashFlow' in stock_cashflow.index:
        #    df_opcashflow = stock_cashflow.loc['Operating Cash Flow']
        elif 'Cash Flowsfromusedin Operating Activities Direct' in stock_cashflow.index:
            df_opcashflow = stock_cashflow.loc['Cash Flowsfromusedin Operating Activities Direct']
            # cols[0] = 'Cash Flowsfromusedin Operating Activities Direct'
        else:
            print('ERROR: no operating cash flow found!')
            exit()

        # get free cashflow
        if 'Free Cash Flow' in stock_cashflow.index:
            df_fcf = stock_cashflow.loc['Free Cash Flow']
        #if 'FreeCashFlow' in stock_cashflow.index:
        #    df_fcf = stock_cashflow.loc['Free Cash Flow']
        else:
            print('ERROR: no free cash flow found!')
            exit()

        if 'Capital Expenditure' in stock_cashflow.index:
            df_capex = stock_cashflow.loc['Capital Expenditure']  # cols.pop(1)
            if pd.DataFrame(df_capex).isnull().values.any():
                df_capex = -1 * (df_opcashflow - df_fcf)
        else:
            df_capex = -1 * (df_opcashflow - df_fcf)

        # if any numbers are postive -> set to 0
        if (df_capex > 0).any():
            print('\tERR: positive CapEx data found, setting stock as failed')
            self.update_stock_list(ticker, stock_info=stock, failed=True)
            return False

        # transpose back to year as index
        cashflow_statements = pd.concat([df_opcashflow, df_capex, df_fcf], axis=1).transpose()

        # if null values, error message and exit
        if cashflow_statements.isnull().values.any():
            print('\tERR: null values detected in cashflow data')
            self.update_stock_list(ticker, stock_info=stock, failed=True)
            return False

        # divide everything by 1000 (we dont care about that level of precision)
        cashflow_statements = cashflow_statements / 1000

        # if the company's numbers are larger max integer size, then dont write values to database
        if (cashflow_statements.abs() > 2147483647).any().any():
            print('Revenue above integer capacity for ticker: {}'.format(ticker))
            self.update_stock_list(ticker, stock_info=stock, failed=True,
                                   comment='Revenue Range > integer capacity')
            return False

        # write each annual cashflow statement to database [optimisation available to write all at once]
        for date in cashflow_statements:
            annual_cf = cashflow_statements[date]
            self.write_cashflow(ticker, date.year, *annual_cf)

        # update stock list with current stock
        # stock_list = self.query("SELECT id FROM ticker WHERE id='{0}' LIMIT 1;".format(ticker))

        # update stock list
        self.update_stock_list(ticker, stock_info=stock, failed=False)

        return True

    def update_asx_list(self):
        # sql to write data [optimisable if pre-check done in sql]
        sql = """INSERT INTO unanalysed_tickers(id, marketcap) VALUES %s RETURNING id;"""

        try:
            # get list of tickers
            results = pyasx.data.companies.get_listed_companies()
        except (Exception,) as e:
            print('l293: {}'.format(e))
            exit()
            self.conn.reset()

        print('generating stock list..')
        data = []
        for stock in results:
            stock_list = self.query("SELECT id FROM ticker WHERE id='{0}' "
                                    "UNION SELECT id FROM unanalysed_tickers WHERE id='{0}' "
                                    "LIMIT 1;".format(stock['ticker'] + '.ax'))
            if len(stock_list) == 0 and stock['market_cap'].isnumeric():
                data.append((stock['ticker'] + '.ax', int(int(stock['market_cap']) / 1000000)))

        if len(data) == 0:
            print('no new asx listings')
            return

        print('\tUpdating unanalysed stock list: adding {} new tickers'.format(len(data)))

        try:
            # run query
            psycopg2.extras.execute_values(self.cursor, sql, data, template=None, page_size=10000)

            # get first result of query
            rs = self.cursor.fetchall()

            print('added {0} new asx listings'.format(len(rs)))

            # commit changes
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as e:
            self.err_print(function='update_asx_list', msg='Failed to write to database', error=e)
            exit()
            self.conn.reset()

    def update_nasdaq_list(self):
        # sql to write data [optimisable if pre-check done in sql]
        sql = """INSERT INTO unanalysed_tickers(id, marketcap) VALUES %s RETURNING id;"""

        # download files
        if os.path.exists("nasdaq.lst"):
            os.remove("nasdaq.lst")
        if os.path.exists("otherlisted.lst"):
            os.remove("otherlisted.lst")

        os.system("curl --ftp-ssl anonymous:jupi@jupi.com "
                  "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt "
                  "> nasdaq.lst")

        os.system("curl --ftp-ssl anonymous:jupi@jupi.com "
                  "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt "
                  "> otherlisted.lst")

        # load data
        try:
            # nasdaq
            colnames = ['ticker', 'name', 'category', 'TestIssue', 'status', 'size', 'IsEtf', 'NextShares']
            nasdaq = pd.read_csv("nasdaq.lst", sep='|', header=None, engine='python', names=colnames)
            # unlisted
            colnames = ['ticker', 'name', 'Exchange', 'CQSsymbol', 'IsEtf', 'RoundLotSize', 'TestIssue', 'NASSymbol']
            otherlisted = pd.read_csv("otherlisted.lst", sep='|', header=None, engine='python', names=colnames)
        except (Exception, ) as e:
            self.err_print(function='update_nasdaq_list', msg='failed to download / read nasdaq files. returning', error=e)
            exit()
            return

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

        # read data into sql list
        data = []
        print('checking if nasdaq stocks already analysed:')
        for index, row in tqdm(nasdaq.iterrows(), total=nasdaq.shape[0], leave=False):
            stock_list = self.query("SELECT id FROM ticker WHERE id='{0}' "
                                    "UNION SELECT id FROM unanalysed_tickers WHERE id='{0}' "
                                    "LIMIT 1;".format(row['ticker']))
            if len(stock_list) == 0:
                data.append((row['ticker'], -1))
        print('\tdone')

        print('checking if unlisted stocks already analysed:')
        for index, row in tqdm(otherlisted.iterrows(), total=otherlisted.shape[0], leave=False):
            stock_list = self.query("SELECT id FROM ticker WHERE id='{0}' "
                                    "UNION SELECT id FROM unanalysed_tickers WHERE id='{0}' "
                                    "LIMIT 1;".format(row['ticker']))
            if len(stock_list) == 0:
                data.append((row['ticker'], -1))
        print('\tdone')

        if len(data) == 0:
            print('no new nasdaq listings')
            return

        try:
            # run query
            psycopg2.extras.execute_values(self.cursor, sql, data, template=None, page_size=10000)

            # get first result of query
            rs = self.cursor.fetchall()

            print('added {0} new nasdaq listings'.format(len(rs)))

            # commit changes
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as e:
            self.err_print(function='update_nasdaq_list', msg='Failed to write to database', error=e)
            exit()
            self.conn.reset()

    def update_hkse_list(self):
        sql = """INSERT INTO unanalysed_tickers(id, marketcap) VALUES %s RETURNING id;"""

        # build list from wiki
        # 'https://en.wikipedia.org/wiki/List_of_companies_listed_on_the_Hong_Kong_Stock_Exchange'

        # get the page Details
        page = wikipedia.page("List of companies listed on the Hong Kong Stock Exchange")
        content = page.html()

        soup = BeautifulSoup(content, "html.parser")
        #for a in soup.findAll('a', href=True, attrs={'class': '_31qSD5'}):
        #    name = a.find('div', attrs={'class': '_3wU53n'})

        data = []
        for table in soup.findAll('table'):
            for a in table.findAll('a', href=True, attrs={'class': 'external text'}):
                if a.text.isnumeric():
                    ticker = '{:04d}'.format(int(a.text)) + '.HK'

                    stock_list = self.query("SELECT id FROM ticker WHERE id='{0}' "
                                            "UNION SELECT id FROM unanalysed_tickers WHERE id='{0}' "
                                            "LIMIT 1;".format(ticker))
                    if len(stock_list) == 0:
                        data.append((ticker, -1))

        if len(data) == 0:
            print('no new hkse listings')
            return

        print('\tUpdating unanalysed stock list: adding {} new tickers'.format(len(data)))

        try:
            # run query
            psycopg2.extras.execute_values(self.cursor, sql, data, template=None, page_size=10000)

            # get first result of query
            rs = self.cursor.fetchall()

            print('added {0} new hkse listings'.format(len(rs)))

            # commit changes
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as e:
            self.err_print(function='update_hkse_list', msg='Failed to write to database', error=e)
            self.conn.reset()

    def update_stock_refresh_list(self):
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
        condition2 = '(update_attempts > 0 AND update_attempts < {attempts} AND {time_condition})'.format(attempts=self.retry_attempts, time_condition=time_condition2)
        condition3 = '(update_attempts > {attempts} AND {time_condition})'.format(attempts=self.retry_attempts, time_condition=time_condition3)

        # combine conditions
        conditions = '{0} OR {1} OR {2}'.format(condition1, condition2, condition3)

        # add conditional constraints to sql query
        sql = base_sql.format(conditions=conditions)

        return self.query(sql)

    def clear_unanalysed_stock(self, ticker):
        sql = """DELETE FROM unanalysed_tickers WHERE id=%s RETURNING id;"""
        print('\tremoving {0} from unanalysed stocks'.format(ticker))

        try:
            # run query
            self.cursor.execute(sql, (ticker,))

            # get first result of query
            rs = self.cursor.fetchall()

            # commit changes
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print('ticker: {0} '.format(ticker))
            print(error)
            exit()
            self.conn.reset()

    def eval_growth(self, ticker):
        self.cursor.execute(
            "SELECT year, operating_cashflow, cap_expenditure, freecashflow FROM cashflow WHERE id='{0}' ORDER BY year;".format(
                ticker))

        # return results
        ticker_data = self.cursor.fetchall()
        ticker_cashflow = pd.DataFrame(ticker_data, columns=['year', 'op_cashflow', 'capex', 'fcf']).set_index('year')
        ticker_cashflow['capex'] = ticker_cashflow['capex'] * -1

        # safe guard against stocks with only 1 year of data
        if len(ticker_cashflow) < 2:
            print('\tInsufficient data to calculate intrinsic value for {0}'.format(ticker))
            return False

        # get op_cash -> if contains negative numbers, make adjustment
        op_cash = ticker_cashflow['op_cashflow'].to_numpy()
        if min(op_cash) <= 0:
            if all(i <= 0 for i in op_cash):
                small = max(op_cash[op_cash < 0]) * -1
            else:
                small = min(number for number in op_cash if number > 0)
            op_cash = op_cash - min(op_cash) + small

        # get capex -> if contains negative numbers, make adjustment
        capex = ticker_cashflow['capex'].to_numpy()
        if min(capex) <= 0:
            if all(i <= 0 for i in capex):
                small = max(capex) * -1
            else:
                small = min(number for number in capex if number > 0)
            capex = capex - min(capex) + small

        # Growth rate G1
        # ticker_cashflow['op_cashflow']
        op_cashflow_growth = []
        cap_ex_growth = []
        for i in range(len(ticker_cashflow) - 1):
            op_cashflow_growth.append(
                #(ticker_cashflow['op_cashflow'].iloc[i + 1] - ticker_cashflow['op_cashflow'].iloc[i]) /
                #ticker_cashflow['op_cashflow'].iloc[i]
                (op_cash[i + 1] - op_cash[i]) / op_cash[i]
            )

            if ticker_cashflow['capex'].iloc[i] != 0:
                cap_ex_growth.append(
                    (ticker_cashflow['capex'].iloc[i + 1] - ticker_cashflow['capex'].iloc[i]) /
                    ticker_cashflow['capex'].iloc[i]
                )
            else:
                cap_ex_growth.append(0)

        # calc g1 growth
        g1_growth = [sum(op_cashflow_growth) / len(op_cashflow_growth), sum(cap_ex_growth) / len(cap_ex_growth)]

        # get op_cash -> if contains negative numbers, make adjustment
        """
        op_cash = ticker_cashflow['op_cashflow'].to_numpy()
        capex = ticker_cashflow['capex'].to_numpy()
        if min(op_cash) <= 0:
            if all(i <= 0 for i in op_cash):
                small = max(op_cash) * -1
            else:
                small = min(number for number in op_cash if number > 0)
            op_cash = op_cash - min(op_cash) + small

        if min(capex) <= 0:
            if all(i <= 0 for i in capex):
                small = max(capex) * -1
            else:
                small = min(number for number in capex if number > 0)
            capex = capex - min(capex) + small
        """

        g2_growth = [0, 0]
        g2_growth[0] = np.polyfit(ticker_cashflow['op_cashflow'].index.to_numpy(),
                                  np.log(op_cash), 1)[0]

        g3_growth = [0, 0]
        g3_growth[0] = np.polyfit(ticker_cashflow['op_cashflow'].index.to_numpy(),
                                  ticker_cashflow['op_cashflow'].to_numpy(), 1)[0] / np.average(
            ticker_cashflow['op_cashflow'])

        # now calculate capex for our growth rates
        if not (ticker_cashflow['capex'] == ticker_cashflow['capex'].iloc[0]).all():
            # calc g1 capex growth
            # ___ no calc required, capex already calculated above

            # calc g2 capex growth
            g2_growth[1] = np.polyfit(ticker_cashflow['capex'].index.to_numpy(),
                                      np.log(capex), 1)[0]
            # np.log(ticker_cashflow['capex'].to_numpy()), 1)[0]

            # calc g3 capex growth
            g3_growth[1] = np.polyfit(ticker_cashflow['capex'].index.to_numpy(),
                                      ticker_cashflow['capex'].to_numpy(), 1)[0] / np.average(ticker_cashflow['capex'])
        else:
            g1_growth[1] = 0
            g2_growth[1] = 0
            g3_growth[1] = 0

        # estimate 10 year growth rate to be half the estimated growth rate
        growth_10yr = min(0.5 * min([g1_growth[0], g2_growth[0], g3_growth[0]]), 0.1)

        # concatenate growthrates into an array for ease of use
        growth_rates = [g1_growth, g2_growth, g3_growth]

        # ensure no insane negative growth values
        for g in range(len(growth_rates)):
            for i in range(2):
                if growth_rates[g][i] < -0.99:
                    growth_rates[g][i] = -0.99

        # predicted growth
        predicted_earnings = [ticker_cashflow['op_cashflow'].iloc[-1]] * len(growth_rates)
        predicted_capex = [ticker_cashflow['capex'].iloc[-1]] * len(growth_rates)

        # intrinsic value estimates
        intrinsic = [0] * len(growth_rates)

        # for each growth estimate calculate the predicated free cash flows
        for g in range(len(growth_rates)):
            for year_dx in range(10):
                # predict likely earnings
                predicted_earnings[g] = predicted_earnings[g] * (
                        1 + growth_rates[g][0] - ((growth_rates[g][0] - growth_10yr) / 9) * year_dx)
                predicted_capex[g] = predicted_capex[g] * (1 + growth_rates[g][1])
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

        intrinsic = np.array(intrinsic)
        intrinsic = intrinsic / 1000
        intrinsic[intrinsic > 2147483647] = 2147483647
        intrinsic[intrinsic < 0] = 0.0
        intrinsic = [round(i) for i in intrinsic]

        print('\tintrinsic value estimate calculated: {col}{value}{e_col}'.format(
            value=millify(intrinsic[1]*1000000),
            col=bcolors.OKGREEN, e_col=bcolors.ENDC))

        # update ticker table
        sql_1 = """UPDATE ticker SET intrinsicvalue_min=%s, intrinsicvalue_best=%s WHERE id=%s RETURNING id;"""
        values_1 = (min(intrinsic), intrinsic[1], ticker)

        # check if intrinsic value table has ticker
        id = self.query("SELECT id FROM intrinsicvalue WHERE id='{0}'".format(ticker))

        # if not exist, add it, else update it
        if len(id) == 0:
            sql_2 = """INSERT INTO intrinsicvalue(id, growth_rate_1a, growth_rate_1b, growth_rate_2a, 
            growth_rate_2b, growth_rate_3a, growth_rate_3b, intrinsicvalue_1, intrinsicvalue_2, 
            intrinsicvalue_3) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning id;"""

            values_2 = (ticker, *g1_growth, *g2_growth, *g3_growth, *intrinsic)
        else:
            sql_2 = """UPDATE intrinsicvalue SET growth_rate_1a=%s, growth_rate_1b=%s, growth_rate_2a=%s, 
            growth_rate_2b=%s, growth_rate_3a=%s, growth_rate_3b=%s, intrinsicvalue_1=%s, intrinsicvalue_2=%s, 
            intrinsicvalue_3=%s RETURNING id;"""
            values_2 = (*g1_growth, *g2_growth, *g3_growth, *intrinsic)

        try:
            # run query
            self.cursor.execute(sql_1, values_1)

            # get first result of query
            rs = self.cursor.fetchone()
            print('\tsuccessfully updated TICKER table intrinsic value for ticker {0}'.format(ticker))

            # run query
            self.cursor.execute(sql_2, values_2)

            # get first result of query
            rs = self.cursor.fetchone()
            print('\tsuccessfully updated INTRINSIC table for ticker {0}'.format(ticker))

            # commit changes
            self.conn.commit()

            return True
        except (Exception, psycopg2.DatabaseError) as e:
            msg = 'Failed to update ticker/intrinsic table for ticker: {0} '.format(ticker)
            self.err_print(function='eval_growth', msg=msg, error=e)
            exit()
            self.conn.reset()
        return False

    @staticmethod
    def err_print(function, msg, error):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        func_name = exc_tb.tb_frame.f_code.co_name
        line_num = exc_tb.tb_lineno

        error_desc = '{type} line {line}: {error}'.format(type=re.split("'", str(exc_type))[1],
                                                          line=line_num,
                                                          error=error)

        string_err = '\t{s_format}[ERR {s_func}] {s_msg}{e_format}\n\t\t{s_err}'
        print(string_err.format(s_func=func_name, s_msg=msg, s_err=error_desc,
                                s_format=bcolors.WARNING, e_format=bcolors.ENDC))


        #print(exc_type, fname, exc_tb.tb_lineno)

def main() -> None:
    # create database object
    pi_sql = Database()

    # pi_sql.write_cashflow('test_s', 2018, 60000, 20000, 40000)
    # pi_sql.write_cashflow('test_s', 2019, 70000, 25000, 45000)
    # pi_sql.write_cashflow('test_s', 2020, 80000, 30000, 50000)
    # pi_sql.write_cashflow('test_s', 2021, 90000, 33000, 57000)
    # pi_sql.update_stock_list('test_s', 'test')
    #pi_sql.eval_growth(ticker)

    # ticker = 'AIS.ax'
    # ticker = 'AKN.ax'
    #ticker = 'ARBK'
    # pi_sql.clear_unanalysed_stock(*ticker)
    #pi_sql.update_stock(ticker)
    # pi_sql.update_stock_list(ticker, 'test', failed=True)

    # get list of stocks
    # ___ generate asx specific list
    #pi_sql.update_asx_list()
    #pi_sql.update_nasdaq_list()
    #pi_sql.update_hkse_list()

    # ___ NAS
    # ... etc

    # for each stock if not in current database -> then add it

    ticker_list = pi_sql.query("SELECT id FROM unanalysed_tickers ORDER BY marketcap DESC")
    ticker_list = ticker_list + pi_sql.update_stock_refresh_list()
    # pi_sql.write_cashflow(ticker, '2018', 4960, -2058, 2902)
    # pi_sql.write_cashflow(ticker, '2017', 4741, -1339, 3401)
    # pi_sql.write_cashflow(ticker, '2016', 2528, -748, 1780)

    # create string to describe next ticker loop
    ticker_opening_string = '\nbeginning new update: ' \
                            'ticker={s_format}{s_ticker}{s_end_format} ' \
                            '[remaining items {s_item_num}/{s_item_count}]'
    ticker_opening_string = ticker_opening_string.format(s_format=bcolors.UNDERLINE + bcolors.BOLD + bcolors.HEADER,
                                                         s_ticker='{}',
                                                         s_end_format=bcolors.ENDC,
                                                         s_item_num='{}',
                                                         s_item_count=len(ticker_list))

    item = 1
    for ticker in ticker_list:
        print(ticker_opening_string.format(ticker[0], item))
        result = pi_sql.update_stock(*ticker)

        # if successful stock update, then eval growth
        if result:
            eval_result = pi_sql.eval_growth(*ticker)
        pi_sql.clear_unanalysed_stock(*ticker)

        item = item + 1


if __name__ == '__main__':
    main()


class Company:
    def __init__(self, ticker: str = ''):
        self.ticker = ticker
        self.exchange = 'au.'  # Temporary
        self.FreeCashFlow = {}
        self.OperatingCashFlow = {}
        self.CapEx = {}

    def fetch(self):
        # my_url = 'https://{Exchange}finance.yahoo.com/quote/{Ticker}/cash-flow?p={Ticker}'
        # my_url = my_url.format(Ticker=self.ticker, Exchange=self.exchange)
        my_url = 'https://au.finance.yahoo.com/'

        print(my_url)
        response = requests.get(my_url)
        print("response.ok : {} , response.status_code : {}".format(response.ok, response.status_code))
        print("Preview of response.text : ", response.text[:500])
        doc = self.get_page(my_url)

    def get_page(self, url):
        """Download a webpage and return a beautiful soup doc"""
        response = requests.get(url)
        if not response.ok:
            print('Status code:', response.status_code)
            raise Exception('Failed to load page {}'.format(url))
        page_content = response.text
        doc = BeautifulSoup(page_content, 'html.parser')
        return doc