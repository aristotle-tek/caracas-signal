import yfinance as yf
import pandas as pd
import os
import logging
from datetime import datetime, timedelta
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw_parquet')

CORE_TICKERS = [
    "XLE",   # Energy Select Sector SPDR Fund
    "USO",   # United States Oil Fund
    "CL=F",  # Crude Oil Futures
    "ITA",   # iShares U.S. Aerospace & Defense ETF
    "FRO",   # Frontline plc (Tankers/Shipping)
    "NAT",   # Nordic American Tankers
    "STNG",  # Scorpio Tankers
    "SPY",   # SPDR S&P 500 ETF Trust (Baseline)
    "HAL",   # Halliburton (Service Contractor)
    "SLB",   # Schlumberger (Service Contractor)
]

def ensure_directories():
    if not os.path.exists(RAW_DIR):
        os.makedirs(RAW_DIR)
        logger.info(f"Created directory: {RAW_DIR}")

def download_ticker_data(
    ticker: str,
    start_date: str,
    end_date: str,
    interval: str = "5m",
    force_refresh: bool = False
) -> Optional[pd.DataFrame]:
    """
    Download intraday data for a ticker; save to Parquet.
    """
    # filename based on params to ensure uniqueness
    safe_ticker = ticker.replace("=", "") # Handle CL=F -> CLF for filenames
    filename = f"{safe_ticker}_{start_date}_{end_date}_{interval}.parquet"
    filepath = os.path.join(RAW_DIR, filename)

    if os.path.exists(filepath) and not force_refresh:
        logger.info(f"Loading cached data for {ticker} from {filepath}")
        try:
            return pd.read_parquet(filepath)
        except Exception as e:
            logger.error(f"Failed to read cached file {filepath}: {e}")
            logger.info("Attempting re-download...")

    logger.info(f"Downloading {ticker} from {start_date} to {end_date} (Interval: {interval})...")
    
    try:
        # yfinance download
        # auto_adjust=True handles splits/dividends, essential for accurate returns
        df = yf.download(
            ticker, 
            start=start_date, 
            end=end_date, 
            interval=interval, 
            auto_adjust=True,
            progress=False
        )

        if df.empty:
            logger.warning(f"No data found for {ticker} in range {start_date}-{end_date}")
            return None

        df.to_parquet(filepath)
        logger.info(f"Saved {ticker} data to {filepath}")
        return df

    except Exception as e:
        logger.error(f"Error downloading {ticker}: {e}")
        return None

def main():
    """
    get data
    """
    ensure_directories()
    
    # Define the forensic window
    # Friday Jan 2, 2026 is the event. 
    # We need:
    # 1. Friday Jan 2 (The Leak)
    # 2. Sunday Jan 4 (The Reaction/Gap)
    # 3. Monday Jan 5 (Confirmation)
    
    start_date = "2026-01-02" 
    end_date = "2026-01-06"
    
    logger.info("Starting forensic data acquisition...")
    
    results = {}
    for ticker in CORE_TICKERS:
        df = download_ticker_data(ticker, start_date, end_date)
        if df is not None:
            results[ticker] = len(df)
            
    logger.info("Data acquisition complete.")
    logger.info(f"Summary of rows fetched: {results}")

def download_daily_baseline():
    """
    Download 90-day daily history for Beta/Z-score calculation.
    """
    start_date = "2025-10-01" # Approx t-90
    end_date = "2026-01-03"   # Up to the event (exclusive)
    
    logger.info("Downloading daily baseline data (T-90)...")
    
    for ticker in CORE_TICKERS:
        download_ticker_data(ticker, start_date, end_date, interval="1d", force_refresh=True)

def download_intraday_baseline():
    """
    Download 30-day intraday history for XLE to validate volume spikes (Placebo Test).
    """
    # 60 day limit for 5m data in yfinance. 
    start_date = "2025-12-01"
    end_date = "2026-01-01"
    
    logger.info("Downloading intraday baseline for XLE, SPY, HAL, SLB, CL=F (Placebo Test & Beta Check)...")
    download_ticker_data("XLE", start_date, end_date, interval="5m")
    download_ticker_data("SPY", start_date, end_date, interval="5m")
    download_ticker_data("HAL", start_date, end_date, interval="5m")
    download_ticker_data("SLB", start_date, end_date, interval="5m")
    download_ticker_data("CL=F", start_date, end_date, interval="5m")

if __name__ == "__main__":
    main()
    download_daily_baseline()
    download_intraday_baseline()
