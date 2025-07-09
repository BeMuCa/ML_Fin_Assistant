"""
This module is responsible for extracting, transforming, and loading financial data.
"""
    
from pyspark.sql import SparkSession

from collect_fin_data import FinanceDataCollector
from db_connector.db_handler import DBHandler
from ml_pipeline import Pipeline

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class FinanceETLPipeline:
    def __init__(self, tickers, period="730d", load_to_db=True):
        self.tickers: list = tickers
        self.period = period
        self.load_to_db = load_to_db
        self.Pipeline = Pipeline
        self.spark = SparkSession.builder\
            .appName("FinanceETL")\
            .config("spark.jars", "B:/1_Berk_Coding/Jars/postgresql-42.7.3.jar") \
            .getOrCreate()
        self.collector = FinanceDataCollector(self.tickers, self.period)
        self.db = DBHandler()

    def run(self):
        for ticker in self.tickers: # iterate over each ticker
            print(f"Processing ticker: {ticker} ...")

            data = self.collector.ticker_data
            df_sma = self.collector.calculate_sma(ticker, data)
            df_ema = self.collector.calculate_ema(ticker, data)
            df_market = self.collector.calculate_movement_metrics(ticker, data)
            df_macd = self.collector.calculate_macd(ticker, data)

            # Convert to Spark DataFrames
            spark_sma = self.spark.createDataFrame(df_sma)
            spark_ema = self.spark.createDataFrame(df_ema)
            spark_market = self.spark.createDataFrame(df_market)
            spark_macd = self.spark.createDataFrame(df_macd)

            # Load to DB
            if self.load_to_db:
                self.db.load_sma(ticker, spark_sma)
                self.db.load_ema(ticker, spark_ema)
                self.db.load_market_move(ticker, spark_market)
                self.db.load_macd(ticker, spark_macd)
                
            labeled_data = self.db.add_bull_bear_label(self.db.combine_indicators(ticker))
            
            
            # ML Pipeline 
            self.Pipeline.setup_pipeline()
            self.Pipeline.split_data(labeled_data)
            self.Pipeline.train_model()
            
            # Evaluation
            self.Pipeline.evaluate_model()
            
            # Saving the Model
            self.Pipeline.save_model(ticker=ticker)
            
            
            
            # Finish the pipeline
            self.spark.stop()
            self.db.close_connection()
            


if __name__ == "__main__":
    tickers = ["AMD", "GOOGL", "MSFT"]  # Example tickers
    period = "730d"  # 2 years of data - max?
    pipeline = FinanceETLPipeline(tickers, period,load_to_db=True)
    pipeline.run()
    print("ETL Pipeline completed successfully.")
    
    
    # Different models trained on different time intervalls 
    # 
    # Dependency between trained time intervalls and how the labels should be set?
    
    # Maybe a 1h is not as effective in predicting 1 week in advance
    
    
    # End goal: 
    # Layer different models trained on different Time intervalls
      