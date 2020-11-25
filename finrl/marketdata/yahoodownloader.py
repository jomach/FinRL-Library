"""Contains methods and classes to collect data from
Yahoo Finance API
"""
import concurrent
from concurrent.futures.thread import ThreadPoolExecutor

import pandas as pd
import yfinance as yf


class YahooDownloader:
    """Provides methods for retrieving daily stock data from
    Yahoo Finance API

    Attributes
    ----------
        start_date : str
            start date of the data (modified from config.py)
        end_date : str
            end date of the data (modified from config.py)
        ticker_list : list
            a list of stock tickers (modified from config.py)

    Methods
    -------
    fetch_data()
        Fetches data from yahoo API

    """
    def __init__(self, 
        start_date:str,
        end_date:str,
        ticker_list:list):

        self.start_date = start_date
        self.end_date = end_date
        self.ticker_list = ticker_list


    def fetch_data(self) -> pd.DataFrame:
        """Fetches data from Yahoo API
        Parameters
        ----------

        Returns
        -------
        `pd.DataFrame` 
            7 columns: A date, open, high, low, close, volume and tick symbol
            for the specified stock ticker
        """
        # Download and save the data in a pandas DataFrame:
        data_df = pd.DataFrame()
        def yf_downloader(tic):
            df = yf.download(tic, start=self.start_date, end=self.end_date)
            df['tic'] = tic
            return df
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(yf_downloader, tic): tic for tic in self.ticker_list}
            for future in concurrent.futures.as_completed(future_to_url):
                try:
                    data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (data, exc))
                else:
                    data_df = data_df.append(data)

        # reset the index, we want to use numbers as index instead of dates
        data_df=data_df.reset_index()
        try:
            # convert the column names to standardized names
            data_df.columns = ['date','open','high','low','close','adjcp','volume','tic']
            # use adjusted close price instead of close price
            data_df['close'] = data_df['adjcp']
            # drop the adjusted close price column
            data_df = data_df.drop('adjcp', 1)
        except NotImplementedError:
            print("the features are not supported currently")

        # convert date to standard string format, easy to filter
        data_df['date']=data_df.date.apply(lambda x: x.strftime('%Y-%m-%d'))
        # drop missing data 
        data_df = data_df.dropna()
        data_df = data_df.reset_index(drop=True)
        print("Shape of DataFrame: ", data_df.shape)
        #print("Display DataFrame: ", data_df.head())

        return data_df


    def select_equal_rows_stock(df):
        df_check=df.tic.value_counts()
        df_check=pd.DataFrame(df_check).reset_index()
        df_check.columns = ['tic','counts']
        mean_df = df_check.counts.mean()
        equal_list=list(df.tic.value_counts() >= mean_df)
        names = df.tic.value_counts().index
        select_stocks_list = list(names[equal_list])
        df=df[df.tic.isin(select_stocks_list)]
        return df