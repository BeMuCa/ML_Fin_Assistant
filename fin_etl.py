"""
This module is responsible for extracting, transforming, and loading financial data.
"""
    
from pyspark.sql import SparkSession

from collect_fin_data import FinanceDataCollector
from db_connector.db_handler import DBHandler
from indicators.simple_indicators import calc_sma, calc_ema, calc_market_move, calc_macd

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class FinanceETLPipeline:
    def __init__(self, tickers, period="730d"):
        self.spark = SparkSession.builder.appName("FinanceETL").getOrCreate()
        self.collector = FinanceDataCollector(tickers, period)
        self.db = DBHandler(dbname="your_db", user="user", password="pw")

    def run(self):
        self.collector.collect_ticker_data()

        for ticker, df in self.collector.ticker_data.items():
            df_sma = calc_sma(df)
            df_ema = calc_ema(df)
            df_market = calc_market_move(df)
            df_macd = calc_macd(df)

            # Optional: convert to Spark DataFrames
            spark_sma = self.spark.createDataFrame(df_sma)
            spark_ema = self.spark.createDataFrame(df_ema)
            spark_market = self.spark.createDataFrame(df_market)
            spark_macd = self.spark.createDataFrame(df_macd)

            # Load to DB
            self.db.load_sma(ticker, spark_sma)
            self.db.load_ema(ticker, spark_ema)
            self.db.load_market_move(ticker, spark_market)
            self.db.load_macd(ticker, spark_macd)
