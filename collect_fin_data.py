"""
Collect financial data for a specific stock.
"""

from db_connector.db_handler import DBHandler
from indicators.simple_indicators import sma_ema_set
import yfinance as yf
from yfinance import Ticker
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession

class FinanceDataCollector:
    """
    Collects financial data for a specific stock ticker.
    """

    def __init__(self, ticker: str, period: str = "10d"):  # TEST 3 YEARS
        self.ticker = ticker
        self.period = period
        #initialize the database handler
        self.db_handler = DBHandler()   
        self.ticker_data: pd.DataFrame = self.get_4h_data(self.ticker, self.period)
        print(f"Ticker: {self.ticker} - Period: {self.period}")

    ####### GET DATA FROM YFINANCE ########
    def get_4h_data(self, ticker="AMD", period="2d"): # 200d minimum for sma ema
        """
        Fetches 4-hour interval data for the specified ticker.
        Includes : Datetime, Open, High, Low, Close, Volume, and 
        9:30 AM to 4:00 PM time range.
        9:30
        13:30
        17:30
        21:30
        """
        stock = yf.Ticker(ticker)
        df: pd.DataFrame = stock.history(interval="4h", period=period)
        df.reset_index(inplace=True)
        return df

    ####### LOAD TO DATABASE ########
    
    def load_movement_metrics_to_db(self,movement_metrics: dict):
        """
        Load movement metrics into the database.
        """
        
        self.db_handler.insert_stock_movement(self.ticker, movement_metrics)
        return 
    
    def load_sma_to_db(self,movement_metrics: dict):
        """
        Load movement metrics into the database.
        """
    
        self.db_handler.insert_sma(self.ticker, movement_metrics)
        return 

    def load_ema_to_db(self,movement_metrics: dict):
        """
        Load movement metrics into the database.
        """
    
        self.db_handler.insert_ema(self.ticker, movement_metrics)
        return 

    def load_macd_to_db(self,movement_metrics: dict):
        """
        Load movement metrics into the database.
        """
        
        self.db_handler.insert_stock_movement(self.ticker, movement_metrics)
        return 

    ######## CALCULATE METRICS ########
    
    def calculate_movement_metrics(self, df):
        """
        Adds volatility, gap, and other movement metrics.
        """
        data = {}
        data["timestamp"] = df["Datetime"]
        data["Volatility"] = df["High"] - df["Low"]
        data["Gap"] = df["Open"] - df["Close"].shift(1)
        data["Gap_Percentage"] = (df["Gap"] / df["Close"].shift(1)) * 100
        data["Daily_High"] = df["High"]
        data["Daily_Low"] = df["Low"]
        data["Daily_Opening"] = df["Open"]
        data["Daily_Closing"] = df["Close"]
        data["Volume"] = df["Volume"]
        return data

    def calculate_sma(self, df):
        """
        Adds EMA and SMA columns to a price DataFrame.
        """
        data = {}
        data["timestamp"] = df["Datetime"]
        data["sma_9"] = df["Close"].rolling(window=9).mean()
        data["sma_50"] = df["Close"].rolling(window=50).mean()
        data["sma_200"] = df["Close"].rolling(window=200).mean()
        return data

    def calculate_ema(self,df):
            """
            Adds EMA and SMA columns to a price DataFrame.
            """
            data = {}
            data["timestamp"] = df["Datetime"]
            data["ema_9"] = df["Close"].ewm(span=9, adjust=False).mean()    # Exponentially weighted moving
            data["ema_50"] = df["Close"].ewm(span=50, adjust=False).mean()
            data["ema_200"] = df["Close"].ewm(span=200, adjust=False).mean()
            return data
        
    def calculate_macd(self, df, short_period=12, long_period=26, signal_period=9):
        """
        Calculates MACD, Signal Line, and Histogram.
        Assumes df has a 'Close' column.
        """
        data = {}
        short_ema = df["Close"].ewm(span=short_period, adjust=False).mean()
        long_ema = df["Close"].ewm(span=long_period, adjust=False).mean()

        macd_line = short_ema - long_ema
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
        histogram = macd_line - signal_line

        data["timestamp"] = df["Datetime"]
        data["macd_line"] = macd_line
        data["macd_signal"] = signal_line
        data["macd_hist"] = histogram

        return data




if __name__ == "__main__":
    ticker = "AMD"
    FDC = FinanceDataCollector(ticker)
    
    a=FDC.ticker_data
    print(len(a))
    b=FDC.calculate_ema(a)
    spark = SparkSession.builder.appName("FinanceETL").getOrCreate()
    print(spark.createDataFrame(b))
    
    # ml kriegt jetzige daten
    # label ist die markt movement im nächsten frame