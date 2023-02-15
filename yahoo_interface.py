import datetime

import pytz
import yahooquery
import yfinance
import pandas as _pd
from util import print_subitems, bcolors


class yahoobase_obj:
    def __init__(self, ticker):
        # set ticker name
        self.ticker = ticker

        self._stock = None
        self._general_info = None
        # _stats -> marketcap, stockprice, sharesoutstanding, profitmargin
        self._stats = None
        # cashflow format pandas df -> index = "Year", columns = ["OperatingCashFlow" "CapEx" "FCF"] (capex show with negative #)
        self._cashflow_statement = None  # return all cashflow in $'000

        self.setup()

    def setup(self):
        self._stock = self._get_stock()  # stock interface from yahoo finance module

    def get_info(self):
        return None

    def get_stats(self) -> dict:
        """ returns a dict object of the format {"market_price", "shares_outstanding", "market_cap", "profit_margin"}"""
        return None

    def get_cashflow(self):
        return None

    def verify_functionality(self):
        tests = {'test_get_stock': False,
                 'test_get_info': False,
                 'test_get_stats': False,
                 'test_get_cashflow': False}

        print('verifying {}'.format(__class__.__name__))

        # begin tests
        print('\tchecking can get stock:')
        stock_obj = self._stock
        if stock_obj is not None:
            tests["test_get_stock"] = True
            print('\t\t{}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        else:
            print('\t\t{}failed{}'.format(bcolors.FAIL, bcolors.ENDC))

        print('\tchecking general information:')
        info = self.info
        if info is not None:
            tests["test_get_info"] = True
            print('\t\t{}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        else:
            print('\t\t{}failed{}'.format(bcolors.FAIL, bcolors.ENDC))

        print('\tchecking company stats:')
        stats = self.stats
        if stats is not None and not all(element == -1 for element in stats.values()):
            tests["test_get_stats"] = True
            print('\t\t{}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        else:
            print('\t\t{}failed{}'.format(bcolors.FAIL, bcolors.ENDC))

        print('\tchecking cashflow:')
        cashflow_data = self.cashflow
        if cashflow_data is not None:
            tests["test_get_cashflow"] = True
            print('\t\t{}passed{}'.format(bcolors.OKGREEN, bcolors.ENDC))
        else:
            print('\t\t{}failed{}'.format(bcolors.FAIL, bcolors.ENDC))

        print("verification results:: passed {}/{} tests".format(sum([1 for test in tests.values() if test]), len(tests.keys())))

        return sum([1 for test in tests.values() if test]) / float(len(tests.keys()))

    def _get_stock(self):
        return None

    # define our properties
    @property
    def info(self):  # -> _pd.DataFrame:
        if self._general_info is None:
            self._general_info = self.get_info()

        return self._general_info

    @property
    def stats(self):  # {"market_price", "shares_outstanding", "market_cap", "profit_margin"}:
        if self._stats is None:
            try:
                self._stats = self.get_stats()
            except pytz.exceptions.UnknownTimeZoneError as e:
                print('{}UnknownTimeZoneError{} returning empty stats'.format(bcolors.FAIL, bcolors.ENDC))
                self._stats = {}

        if self._stats == {}:
            return None

        return self._stats

    @property
    def cashflow(self):  # -> _pd.DataFrame:
        if self._cashflow_statement is None:
            self._cashflow_statement = self.get_cashflow()
        elif self._cashflow_statement.empty:
            return None

        return self._cashflow_statement


class yfin_obj(yahoobase_obj):
    def __init__(self, ticker):
        super().__init__(ticker)

        self._asset_info = None
        self._fast_info = None
        self._cashflow_info = None

    def get_info(self) -> dict:
        """ returns a dict object of the format {"ticker", "name", "sector", "country"} """
        info = {}

        stock_general_info = self._get_asset_info()

        if stock_general_info is None:
            return None

        # get ticker
        info["ticker"] = self.ticker

        # get name
        if "longBusinessSummary" in stock_general_info.keys():
            test_separators = ["limited", "inc.", "ltd.", "corp.", ",", "EOL"]

            for sep in test_separators:
                if sep in stock_general_info["longBusinessSummary"][:32].lower():
                    info["name"] = stock_general_info["longBusinessSummary"].lower().split(sep)[0].strip()
                    break

                if sep == "EOL":
                    print('failed to generate name from\n\t{}'.format(stock_general_info["longBusinessSummary"]))
                    info["name"] = "NA"
        else:
            info["name"] = "NA"

        # get sector
        if "sector" in stock_general_info.keys():
            info["sector"] = stock_general_info["sector"]
        else:
            info["sector"] = "NA"

        if "country" in stock_general_info.keys():
            info["country"] = stock_general_info["country"]
        else:
            info["country"] = "NA"

        return info

    def get_stats(self) -> dict:
        """ returns a dict object of the format {"market_price", "shares_outstanding", "market_cap", "profit_margin"} """
        stats = {}

        stock_fast_info = self._get_fast_info()
        stock_general_info = self._get_asset_info()

        if stock_fast_info is None or stock_general_info is None:
            print('fast_info was none - returning a none value')
            return {}

        if "previousClose" in stock_fast_info.keys():
            stats["market_price"] = stock_fast_info["previousClose"]
        elif "regularMarketPreviousClose" in stock_fast_info.keys():
            stats["market_price"] = stock_fast_info["regularMarketPreviousClose"]
        else:
            stats["market_price"] = -1.0
        if "shares" in stock_fast_info.keys():
            stats["shares_outstanding"] = stock_fast_info["shares"]
        else:
            stats["shares_outstanding"] = -1
        if "marketCap" in stock_fast_info.keys():
            stats["market_cap"] = stock_fast_info["marketCap"]
        else:
            stats["market_cap"] = -1
        if "profitMargins" in stock_general_info.keys():
            stats["profit_margin"] = stock_general_info["profitMargins"]
        else:
            stats["profit_margin"] = -1.0

        if any(value is None for value in stats.values()):
            print('fast_info contains None - returning a none value')
            return {}

        stats["market_price"] = round(float(stats["market_price"]), 2)
        stats["shares_outstanding"] = int(stats["shares_outstanding"])
        stats["market_cap"] = int(stats["market_cap"])
        stats["profit_margin"] = round(float(stats["profit_margin"]), 2)

        return stats

    def get_cashflow(self):
        cashflow = self._get_cashflow_info()

        # transpose it so that it matches initial format of yahooquery results
        cashflow = cashflow.transpose()

        # remove unuseful columns
        if len(cashflow.keys()) == 0:
            print('cashflow got no data')
            return None
        #print(self.ticker, 'printing keys')
        #print_subitems(cashflow.keys())
        if "CashFlowFromContinuingOperatingActivities" in cashflow.keys():
            operatingCF = "CashFlowFromContinuingOperatingActivities"
        elif "CashFlowsfromusedinOperatingActivitiesDirect" in cashflow.keys():
            operatingCF = "CashFlowsfromusedinOperatingActivitiesDirect"
        elif "OperatingCashFlow" in cashflow.keys():
            operatingCF = "OperatingCashFlow"
        else:
            print(self.ticker, 'failed to get cashflow, no valid keys found for operatingcf')
            for key in cashflow.keys():
                print(key)
            return None

        columns = [operatingCF, "CapitalExpenditure", "FreeCashFlow"]
        cashflow = _pd.DataFrame(cashflow, columns=columns)

        # rename columns for consistent output
        cashflow.rename(columns={'asOfDate': 'Year',
                                 operatingCF: 'OperatingCashFlow',
                                 'CapitalExpenditure': 'CapEx',
                                 'FreeCashFlow': 'FCF'}, inplace=True)

        # set Year as the index
        cashflow = cashflow.transpose()
        cashflow.index.name = "Year"

        return cashflow

    # DEFINE MAIN RAW DATA COLLECTION FUNCTIONS
    def _get_stock(self):
        if self._stock is None:
            self._stock = yfinance.Ticker(self.ticker)

        return self._stock

    def _get_asset_info(self):
        if self._asset_info is None:
            self._asset_info = self._stock.info

        return self._asset_info

    def _get_fast_info(self):
        if self._fast_info is None:
            self._fast_info = self._stock.fast_info

        return self._fast_info

    def _get_cashflow_info(self):
        if self._cashflow_info is None:
            self._cashflow_info = self._stock.get_cash_flow(pretty=False, freq="yearly")

        return self._cashflow_info


class yquery_obj(yahoobase_obj):
    def __init__(self, ticker):
        super().__init__(ticker)

        self._asset_profile = None
        self._key_stats = None
        self._summary_detail = None
        self._cashflow_info = None

    def get_info(self) -> dict:
        """ returns a dict object of the format {"ticker", "name", "sector", "country"} """
        info = {}

        stock_general_info = self._get_asset_profile()

        if stock_general_info is None or type(stock_general_info) is str:
            return None

        # get ticker
        info["ticker"] = self.ticker

        # get name
        if "longBusinessSummary" in stock_general_info.keys():
            test_separators = ["limited", "inc.", "ltd.", "corp.", ",", "EOL"]

            for sep in test_separators:
                if sep in stock_general_info["longBusinessSummary"][:32].lower():
                    info["name"] = stock_general_info["longBusinessSummary"].lower().split(sep)[0].strip()
                    break

                if sep == "EOL":
                    print('failed to generate name from\n\t{}'.format(stock_general_info["longBusinessSummary"]))
                    info["name"] = "NA"
        else:
            info["name"] = "NA"

        # get sector
        if "sector" in stock_general_info.keys():
            info["sector"] = stock_general_info["sector"]
        else:
            info["sector"] = "NA"

        if "country" in stock_general_info.keys():
            info["country"] = stock_general_info["country"]
        else:
            info["country"] = "NA"

        return info

    def get_stats(self) -> dict:
        """ returns a dict object of the format {"market_price", "shares_outstanding", "market_cap", "profit_margin"} """
        stats = {"market_price": None, "shares_outstanding": None, "market_cap": None, "profit_margin": None}

        stock_stats = self._get_key_stats()
        stock_summary_detail = self._get_summary_detail()

        if stock_stats is None or type(stock_stats) is str:
            return None
        if stock_summary_detail is None or type(stock_summary_detail) is str:
            return None

        if "previousClose" in stock_summary_detail.keys():
            stats["market_price"] = round(float(stock_summary_detail["previousClose"]), 2)
        elif "regularMarketPreviousClose" in stock_summary_detail.keys():
            stats["market_price"] = round(float(stock_summary_detail["regularMarketPreviousClose"]), 2)
        else:
            stats["market_price"] = -1.0

        if "marketCap" in stock_summary_detail.keys():
            stats["market_cap"] = int(stock_summary_detail["marketCap"])
        else:
            stats["market_cap"] = -1

        if stats["market_price"] != -1 and stats["market_cap"]!=-1:
            stats["shares_outstanding"] = int(stats["market_cap"] / float(stats["market_price"]))
        elif "sharesOutstanding" in stock_stats.keys():
            stats["shares_outstanding"] = int(stock_stats["sharesOutstanding"])
        else:
            stats["shares_outstanding"] = -1

        if "profitMargins" in stock_stats.keys():
            stats["profit_margin"] = round(float(stock_stats["profitMargins"]), 2)
        else:
            stats["profit_margin"] = -1.0

        return stats

    def get_cashflow(self):
        cashflow = self._get_cashflow_info()

        if type(cashflow) is str:
            return _pd.DataFrame()

        # remove TTM (we dont need current year information before year is complete)
        cashflow = cashflow[cashflow.periodType == "12M"]

        # sort by date from latest to oldest year (descending)
        cashflow = cashflow.sort_values(by="asOfDate", ascending=False)

        # remove unuseful columns
        if "CashFlowFromContinuingOperatingActivities" in cashflow.keys():
            operatingCF = "CashFlowFromContinuingOperatingActivities"
        elif "CashFlowsfromusedinOperatingActivitiesDirect" in cashflow.keys():
            operatingCF = "CashFlowsfromusedinOperatingActivitiesDirect"
        elif "OperatingCashFlow" in cashflow.keys():
            operatingCF = "OperatingCashFlow"
        else:
            print('failed to get cashflow, no valid keys found for operatingcf')
            for key in cashflow.keys():
                print(key)
            return None

        columns = ["asOfDate", operatingCF, "CapitalExpenditure", "FreeCashFlow"]
        cashflow = _pd.DataFrame(cashflow, columns=columns)

        # rename columns for consistent output
        cashflow.rename(columns={'asOfDate': 'Year',
                                 operatingCF: 'OperatingCashFlow',
                                 'CapitalExpenditure': 'CapEx',
                                 'FreeCashFlow': 'FCF'}, inplace=True)

        # set Year as the index
        cashflow.set_index("Year", inplace=True)
        cashflow = cashflow.transpose()

        return cashflow

    # DEFINE MAIN RAW DATA COLLECTION FUNCTIONS
    def _get_stock(self):
        if self._stock is None:
            self._stock = yahooquery.Ticker(self.ticker)

        return self._stock

    def _get_key_stats(self) -> dict:
        if self._key_stats is None:
            self._key_stats = self._stock.key_stats[self.ticker]

        return self._key_stats

    def _get_asset_profile(self) -> dict:
        if self._asset_profile is None:
            self._asset_profile = self._stock.asset_profile[self.ticker]

        return self._asset_profile

    def _get_summary_detail(self) -> dict:
        if self._summary_detail is None:
            self._summary_detail = self._stock.summary_detail[self.ticker]

        return self._summary_detail

    def _get_cashflow_info(self):
        if self._cashflow_info is None:
            self._cashflow_info = self._stock.cash_flow(frequency="a")

        return self._cashflow_info


if __name__ == '__main__':
    #  run test on classes
    ticker = 'TSLA'
    ticker = 'NXT.ax'
    ticker = '0700.HK'
    ticker = 'MSFT'
    ticker = 'GOOG'
    ticker = 'PME.ax'

    t_yahoofin = yfin_obj(ticker)
    results1 = t_yahoofin.verify_functionality()

    t_yahooquery = yquery_obj(ticker)
    results2 = t_yahooquery.verify_functionality()

    print('test results were:', results1)
    print('test results were:', results2)

    i1 = t_yahoofin.info
    s1 = t_yahoofin.stats
    c1 = t_yahoofin.cashflow

    i2 = t_yahooquery.info
    s2 = t_yahooquery.stats
    c2 = t_yahooquery.cashflow

    print(i1)
    print(i2)
    print()
    print(s1)
    print(s2)
    print()
    print(c1)
    print()
    print(c2)

    print()
    print(c1.loc["OperatingCashFlow"][0])
    print(c2.loc["OperatingCashFlow"][0])
