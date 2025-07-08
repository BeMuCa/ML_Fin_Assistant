"""
Collect financial data for a specific stock.
"""

from db_connector.db_handler import DBHandler
from indicators.simple_indicators import sma_ema_set
from yfinance import Ticker
import pandas as pd
from datetime import datetime





timestamp = current_time = datetime.now()


