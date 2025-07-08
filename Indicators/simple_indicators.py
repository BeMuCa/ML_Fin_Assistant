"""
    Purpose: This module contains functions to calculate various financial indicators.

"""

def sma_ema_set(prices, periods=[200, 50, 9]):
    """
    Calculate Simple Moving Averages (SMA) and Exponential Moving Averages (EMA) for given periods.

    :param prices: List of prices.
    :return: Tuple of short SMA, long SMA, and EMA lists.
    """
    short_sma, long_sma = calculate_sma_cross(prices)
    ema = calculate_ema(prices, 9)

    return short_sma, long_sma, ema

def calculate_sma_cross(prices, short_period=50, long_period=200):
    """
    Calculate Simple Moving Average (SMA) cross.

    :param prices: List of prices.
    :param short_period: Short period for the SMA.
    :param long_period: Long period for the SMA.
    :return: Tuple of short SMA and long SMA lists.
    """
    if len(prices) < long_period:
        return [], []

    short_sma = calculate_sma(prices, short_period)
    long_sma = calculate_sma(prices, long_period)

    return short_sma, long_sma


# 200er, 50er, 9er   
def calculate_sma(prices, period):
    """
    Calculate Simple Moving Average (SMA).

    :param prices: List of prices.
    :param period: Period for the SMA.
    :return: List of SMA values.
    """
    if len(prices) < period:
        return []

    sma = []
    for i in range(len(prices)):
        if i < period - 1:
            sma.append(None)
        else:
            sma.append(sum(prices[i - period + 1:i + 1]) / period)
    
    return sma

def calculate_ema(prices, period):
    """
    Calculate Exponential Moving Average (EMA).

    :param prices: List of prices.
    :param period: Period for the EMA.
    :return: List of EMA values.
    """
    if len(prices) < period:
        return []

    ema = [None] * (period - 1)
    sma = sum(prices[:period]) / period
    ema.append(sma)

    multiplier = 2 / (period + 1)
    
    for price in prices[period:]:
        ema_value = (price - ema[-1]) * multiplier + ema[-1]
        ema.append(ema_value)
    
    return ema

def calculate_macd(prices, short_period=12, long_period=26, signal_period=9):
    """
    Calculate Moving Average Convergence Divergence (MACD).

    :param prices: List of prices.
    :param short_period: Short period for the MACD.
    :param long_period: Long period for the MACD.
    :param signal_period: Signal period for the MACD.
    :return: Tuple of MACD line and Signal line.
    """
    if len(prices) < long_period:
        return [], []

    short_ema = calculate_ema(prices, short_period)
    long_ema = calculate_ema(prices, long_period)

    macd_line = [s - l for s, l in zip(short_ema[long_period - 1:], long_ema[long_period - 1:])]
    
    signal_line = calculate_ema(macd_line, signal_period)
    
    return macd_line, signal_line

def calculate_gap_percentage(closing_price, opening_price):
    """
    Calculate the gap percentage between closing and opening prices.

    :param closing_price: Closing price of the previous period.
    :param opening_price: Opening price of the current period.
    :return: Gap percentage value.
    """
    if closing_price is None or opening_price is None:
        return None
    closing_price = float(closing_price)
    opening_price = float(opening_price)
    gap = opening_price - closing_price
    if closing_price == 0:
        return None
    return (gap / closing_price) * 100

def gap_closed(closing_price, opening_price, current_price):
    """
    Determine if the price closed the gap.
    """
    
def daily_opening_price(prices:list):
    """
    Get the opening price of the day from the list of prices.

    :param prices: List of prices.
    :return: Opening price of the day.
    """
    if not prices:
        return None
    return prices[0]  # Assuming the first price in the list is the opening price

def daily_closing_price(prices: list):
    """
    Get the closing price of the day from the list of prices.

    :param prices: List of prices.
    :return: Closing price of the day.
    """
    if not prices:
        return None
    return prices[-1]  # Assuming the last price in the list is the closing price

def calculate_gap(closing_price, opening_price):
    """
    Calculate the gap between closing and opening prices.

    :param closing_price: Closing price of the previous period.
    :param opening_price: Opening price of the current period.
    :return: Gap value.
    """
    if closing_price is None or opening_price is None:
        return None
    closing_price = float(closing_price)
    opening_price = float(opening_price)
    gap = opening_price - closing_price
    return gap
    #if gap >0:
    #    return 1 #"bullish"
    #return -1 #"bearish"



# Too complex for now, needs more data
#
def volume_weighted_moving_average_convergence_divergence(prices, volumes, short_period=12, long_period=26, signal_period=9):
    """
    Calculate Volume Weighted Moving Average Convergence Divergence (VWMACD).

    :param prices: List of prices.
    :param volumes: List of volumes.
    :param short_period: Short period for the VWMACD.
    :param long_period: Long period for the VWMACD.
    :param signal_period: Signal period for the VWMACD.
    :return: Tuple of VWMACD line and Signal line.
    """
    if len(prices) < long_period or len(volumes) < long_period:
        return [], []

    weighted_prices = [p * v for p, v in zip(prices, volumes)]
    
    short_ema = calculate_ema(weighted_prices, short_period)
    long_ema = calculate_ema(weighted_prices, long_period)

    macd_line = [s - l for s, l in zip(short_ema[long_period - 1:], long_ema[long_period - 1:])]
    
    signal_line = calculate_ema(macd_line, signal_period)
    
    return macd_line, signal_line


if __name__ == "__main__":
    pass 