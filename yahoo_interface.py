import yahooquery
import yfinance
import pandas as _pd
from util import print_subitems

class yfin_obj:
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
        """ returns a dict object of the format {"shares_outstanding", "market_cap", "profit_margin"} """
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
            print('\t\tpassed')
        else:
            print('\t\tfailed')

        print('\tchecking general information:')
        info = self.info
        if info is not None:
            tests["test_get_info"] = True
            print('\t\tpassed')
        else:
            print('\t\tfailed')

        print('\tchecking company stats:')
        stats = self.stats
        if stats is not None and not all(element == -1 for element in stats.values()):
            tests["test_get_stats"] = True
            print('\t\tpassed')
        else:
            print('\t\tfailed')

        print('\tchecking cashflow:')
        cashflow_data = self.cashflow
        if cashflow_data is not None:
            tests["test_get_cashflow"] = True
            print('\t\tpassed')
        else:
            print('\t\tfailed')

        print("")

    def _get_stock(self):
        return None

    # define our properties
    @property
    def info(self):  # -> _pd.DataFrame:
        if self._general_info is None:
            self._general_info = self.get_info()

        return self._general_info

    @property
    def stats(self):  # -> _pd.DataFrame:
        if self._stats is None:
            self._stats = self.get_stats()

        return self._stats

    @property
    def cashflow(self):  # -> _pd.DataFrame:
        if self._cashflow_statement is None:
            self._cashflow_statement = self.get_cashflow()

        return self._cashflow_statement

class yquery_obj(yfin_obj):
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
        """ returns a dict object of the format {"shares_outstanding", "market_cap", "profit_margin"} """
        stats = {}

        stock_stats = self._get_key_stats()
        stock_summary_detail = self._get_summary_detail()

        if stock_stats is None:
            return None

        if "previousClose" in stock_summary_detail.keys():
            stats["market_price"] = stock_summary_detail["previousClose"]
        elif "regularMarketPreviousClose" in stock_summary_detail.keys():
            stats["market_price"] = stock_summary_detail["regularMarketPreviousClose"]
        else:
            stats["market_price"] = -1

        if "sharesOutstanding" in stock_stats.keys():
            stats["shares_outstanding"] = stock_stats["sharesOutstanding"]
        else:
            stats["shares_outstanding"] = -1

        if "marketCap" in stock_summary_detail.keys():
            stats["market_cap"] = stock_summary_detail["marketCap"]
        else:
            stats["market_cap"] = -1

        if "profitMargins" in stock_stats.keys():
            stats["profit_margin"] = stock_stats["profitMargins"]
        else:
            stats["profit_margin"] = -1

        return stats

    def get_cashflow(self):
        cashflow = self._get_cashflow_info()

        # remove TTM (we dont need current year information before year is complete)
        cashflow = cashflow[cashflow.periodType == "12M"]

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

        # remove month and day form Year column
        cashflow["Year"] = cashflow["Year"].astype(str).str[:4]

        # set Year as the index
        cashflow.set_index("Year", inplace=True)

        return cashflow

    # DEFINE MAIN RAW DATA COLLECTION FUNCTIONS
    def _get_stock(self):
        if self._stock is None:
            self._stock = yahooquery.Ticker(self.ticker)

        return self._stock

    def _get_key_stats(self):
        if self._key_stats is None:
            self._key_stats = self._stock.key_stats[self.ticker]

        return self._key_stats

    def _get_asset_profile(self):
        if self._asset_profile is None:
            self._asset_profile = self._stock.asset_profile[self.ticker]

        return self._asset_profile

    def _get_summary_detail(self):
        if self._summary_detail is None:
            self._summary_detail = self._stock.summary_detail[self.ticker]

        return self._summary_detail

    def _get_cashflow_info(self):
        if self._cashflow_info is None:
            self._cashflow_info = self._stock.cash_flow(frequency="a")

        return self._cashflow_info


if __name__ == '__main__':
    #  run test on classes
    ticker = 'PME.ax'
    ticker = 'TSLA'
    ticker = 'NXT.ax'
    ticker = '0700.HK'
    ticker = 'MSFT'
    ticker = 'GOOG'

    t_yahooquery = yquery_obj(ticker)
    #t_yahooquery.verify_functionality()
    i = t_yahooquery.info
    s = t_yahooquery.stats
    c = t_yahooquery.cashflow

    print(i)
    print(s)
    print(c)

    #print_subitems(t_yahooquery._stock.cash_flow().keys())
    #print_subitems(t_yahooquery._stock.asset_profile[ticker])
    #print_subitems(t_yahooquery._stock.summary_detail[ticker])
    #print_subitems(t_yahooquery._stock.summary_profile[ticker])
    #print(t_yahooquery._stock.fund_equity_holdings[ticker])
    #print_subitems(t_yahooquery._stock.key_stats[ticker])
    #print_subitems(t_yahooquery._stock.key_stats[ticker])
