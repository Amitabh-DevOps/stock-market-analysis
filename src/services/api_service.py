# API Service module for fetching stock data from Alpha Vantage

import logging
import time
import random
import requests
import pandas as pd
from typing import Optional, Dict
from src import config

logger = logging.getLogger(__name__)


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
    # Simple retry/backoff strategy for transient errors and rate limits
    max_attempts = 3
    base_backoff = 1.0

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info("Fetching intraday data for %s @ %s (attempt %d)", symbol, interval, attempt)

            params = {
                "function": "TIME_SERIES_INTRADAY",
                "symbol": symbol,
                "interval": interval,
                "outputsize": "full",
                "apikey": api_key
            }

            response = requests.get(config.ALPHA_VANTAGE_BASE_URL, params=params, timeout=10)
            status = response.status_code

            # Successful response
            if status == 200:
                try:
                    data = response.json()
                except Exception as e:
                    # Non-JSON response (HTML or proxy page)
                    snippet = (response.text or "")[:500].replace('\n', ' ')
                    logger.warning("Non-JSON response received from Alpha Vantage: %s", snippet)
                    raise ValueError("Unexpected response from data provider.")

                # Handle API-level messages
                if "Error Message" in data:
                    raise ValueError(f"Invalid symbol: {symbol}")
                if "Note" in data:
                    # Rate limit notice — retry a few times then surface error
                    note = data.get("Note", "Rate limit or notice from API")
                    logger.warning("Alpha Vantage Note received: %s", note)
                    if attempt < max_attempts:
                        sleep_for = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                        logger.info("Retrying after %.1fs due to API Note", sleep_for)
                        time.sleep(sleep_for)
                        continue
                    raise ValueError("API rate limit reached. Please wait and try again.")

                return data

            # Rate limiting or server errors — retry
            if status == 429 or (500 <= status < 600):
                snippet = (response.text or "")[:500].replace('\n', ' ')
                logger.warning("Received HTTP %s from Alpha Vantage; snippet: %s", status, snippet)
                if attempt < max_attempts:
                    sleep_for = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                    logger.info("Retrying after %.1fs (status %d)", sleep_for, status)
                    time.sleep(sleep_for)
                    continue
                if status == 429:
                    raise ValueError("API rate limit exceeded. Please wait before making more requests.")
                raise ValueError(f"HTTP error occurred: {status}")

            # Unauthorized
            if status == 401:
                logger.warning("Received 401 Unauthorized from Alpha Vantage")
                raise ValueError("Invalid API key. Please check your configuration.")

            # Other client errors — surface immediately
            snippet = (response.text or "")[:500].replace('\n', ' ')
            logger.warning("Unexpected status %s from Alpha Vantage; snippet: %s", status, snippet)
            raise ValueError(f"HTTP error occurred: {status}")

        except requests.exceptions.Timeout as e:
            logger.exception("Timeout while fetching data: %s", e)
            if attempt < max_attempts:
                time.sleep(base_backoff * (2 ** (attempt - 1)))
                continue
            raise ValueError("Request timed out. Please try again.")
        except requests.exceptions.ConnectionError as e:
            logger.exception("Connection error while fetching data: %s", e)
            if attempt < max_attempts:
                time.sleep(base_backoff * (2 ** (attempt - 1)))
                continue
            raise ValueError("Network connection error. Please check your internet connection.")
        except ValueError:
            # propagate ValueError (invalid symbol, rate limit, etc.) immediately
            raise
        except Exception as e:
            logger.exception("Unexpected error while fetching intraday data: %s", e)
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
