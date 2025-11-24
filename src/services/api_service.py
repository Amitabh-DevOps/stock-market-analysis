# API Service module for fetching stock data from Alpha Vantage

import requests
import pandas as pd
from typing import Optional, Dict
from src import config


def fetch_intraday_data(symbol: str, interval: str, api_key: str = config.ALPHA_VANTAGE_API_KEY) -> Optional[Dict]:
    """
    Fetch intraday time series data from Alpha Vantage API.
    
    Args:
        symbol: Stock symbol (e.g., 'IBM', 'AAPL')
        interval: Time interval ('1min', '5min', '15min', '30min', '60min')
        api_key: Alpha Vantage API key
        
    Returns:
        JSON response dict or None if error occurs
    """
    try:
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": interval,
            "outputsize": "full",
            "apikey": api_key
        }
        
        response = requests.get(config.ALPHA_VANTAGE_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Check for API error messages
        if "Error Message" in data:
            raise ValueError(f"Invalid symbol: {symbol}")
        if "Note" in data:
            raise ValueError("API rate limit reached. Please wait and try again.")
            
        return data
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            raise ValueError("Invalid API key. Please check your configuration.")
        elif e.response.status_code == 429:
            raise ValueError("API rate limit exceeded. Please wait before making more requests.")
        else:
            raise ValueError(f"HTTP error occurred: {e}")
    except requests.exceptions.ConnectionError:
        raise ValueError("Network connection error. Please check your internet connection.")
    except requests.exceptions.Timeout:
        raise ValueError("Request timed out. Please try again.")
    except Exception as e:
        raise ValueError(f"An error occurred: {str(e)}")


def parse_time_series(response: Dict) -> pd.DataFrame:
    """
    Convert API response to pandas DataFrame.
    
    Args:
        response: JSON response from Alpha Vantage API
        
    Returns:
        DataFrame with columns: timestamp, open, high, low, close, volume
    """
    if not response:
        return pd.DataFrame()
    
    # Find the time series key (varies by interval)
    time_series_key = None
    for key in response.keys():
        if key.startswith("Time Series"):
            time_series_key = key
            break
    
    if not time_series_key:
        return pd.DataFrame()
    
    time_series = response[time_series_key]
    
    # Convert to DataFrame
    df = pd.DataFrame.from_dict(time_series, orient='index')
    
    # Rename columns
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    
    # Convert index to datetime
    df.index = pd.to_datetime(df.index)
    df.index.name = 'timestamp'
    
    # Convert columns to numeric
    df['open'] = pd.to_numeric(df['open'])
    df['high'] = pd.to_numeric(df['high'])
    df['low'] = pd.to_numeric(df['low'])
    df['close'] = pd.to_numeric(df['close'])
    df['volume'] = pd.to_numeric(df['volume'])
    
    # Sort by timestamp
    df = df.sort_index()
    
    return df
