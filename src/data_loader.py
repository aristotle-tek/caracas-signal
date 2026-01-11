import argparse
import logging
import os

import pandas as pd
import yfinance as yf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
RAW_DIR = os.path.join(DATA_DIR, "raw_parquet")
os.makedirs(RAW_DIR, exist_ok=True)

# Core tickers
PRIMARY_TICKERS = ["XLE", "SPY", "CL=F"]
ROBUSTNESS_TICKERS = ["BZ=F", "^VIX", "UUP", "USO", "BNO", "VDE", "XOP"]
SECTOR_TICKERS = ["ITA", "FRO", "NAT", "STNG", "BDRY"]


def safe_ticker(ticker):
    return ticker.replace("=", "").replace("^", "")


def download_ticker_data(ticker, start_date, end_date, interval):
    filename = f"{safe_ticker(ticker)}_{start_date}_{end_date}_{interval}.parquet"
    filepath = os.path.join(RAW_DIR, filename)

    logger.info(f"Downloading {ticker} ({interval}) {start_date} to {end_date}")
    try:
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            interval=interval,
            auto_adjust=True,
            progress=False,
        )
        if df.empty:
            logger.warning(f"No data returned for {ticker}")
            return
        df.to_parquet(filepath)
        logger.info(f"Saved {len(df)} rows to {filepath}")
    except Exception as exc:
        logger.error(f"Failed to download {ticker}: {exc}")


def download_baseline(start_date, end_date, interval, tickers):
    for t in tickers:
        download_ticker_data(t, start_date, end_date, interval)


def download_event_day(event_date, tickers):
    # yfinance 1-minute data is typically limited to ~7 days history.
    start_date = event_date
    end_date = (pd.Timestamp(event_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    for t in tickers:
        download_ticker_data(t, start_date, end_date, "1m")


def main():
    parser = argparse.ArgumentParser(description="Download intraday data for replication.")
    parser.add_argument("--event-date", default="2026-01-02", help="Event date (YYYY-MM-DD).")
    parser.add_argument("--baseline-start", default="2025-11-15", help="Baseline start date.")
    parser.add_argument("--baseline-end", default="2025-12-31", help="Baseline end date.")
    parser.add_argument(
        "--tickers",
        default="all",
        help="Tickers to download: all|primary|robustness|sectors",
    )
    args = parser.parse_args()

    if args.tickers == "primary":
        tickers = PRIMARY_TICKERS
    elif args.tickers == "robustness":
        tickers = ROBUSTNESS_TICKERS
    elif args.tickers == "sectors":
        tickers = SECTOR_TICKERS
    else:
        tickers = PRIMARY_TICKERS + ROBUSTNESS_TICKERS + SECTOR_TICKERS

    download_baseline(args.baseline_start, args.baseline_end, "5m", tickers)
    download_event_day(args.event_date, tickers)


if __name__ == "__main__":
    main()
