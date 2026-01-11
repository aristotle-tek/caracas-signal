import os
import logging
import numpy as np
import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import plot_style

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "out")
os.makedirs(OUTPUT_DIR, exist_ok=True)

SECTOR_TICKERS = [
    "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY"
]
MARKET_TICKER = "SPY"
EVENT_DATE = pd.Timestamp("2026-01-02")

def download_prices(tickers, start, end):
    data = {}
    for ticker in tickers:
        try:
            df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
            if df.empty:
                logger.warning(f"No data for {ticker}")
                continue
            data[ticker] = df[["Close"]].copy()
        except Exception as e:
            logger.warning(f"Failed to download {ticker}: {e}")
    return data

def compute_beta(asset_returns, market_returns):
    common = asset_returns.index.intersection(market_returns.index)
    if len(common) < 30:
        return None
    x = market_returns.loc[common]
    y = asset_returns.loc[common]
    if isinstance(x, pd.DataFrame):
        x = x.iloc[:, 0]
    if isinstance(y, pd.DataFrame):
        y = y.iloc[:, 0]
    var = np.var(x, ddof=1)
    if var == 0:
        return None
    cov = np.cov(y, x, ddof=1)[0, 1]
    return cov / var

def run_sector_check():
    plot_style.apply_plot_style()
    start = EVENT_DATE - pd.Timedelta(days=400)
    end = EVENT_DATE + pd.Timedelta(days=5)

    prices = download_prices(SECTOR_TICKERS + [MARKET_TICKER], start=start, end=end)
    if MARKET_TICKER not in prices:
        logger.warning("Missing SPY data for sector check.")
        return

    market = prices[MARKET_TICKER]["Close"]
    market_returns = market.pct_change().dropna()
    if isinstance(market_returns, pd.DataFrame):
        market_returns = market_returns.iloc[:, 0]

    rows = []
    for ticker in SECTOR_TICKERS:
        if ticker not in prices:
            continue
        asset = prices[ticker]["Close"]
        asset_returns = asset.pct_change().dropna()
        if isinstance(asset_returns, pd.DataFrame):
            asset_returns = asset_returns.iloc[:, 0]

        beta = compute_beta(asset_returns[asset_returns.index < EVENT_DATE], market_returns[market_returns.index < EVENT_DATE])
        if beta is None:
            continue

        if EVENT_DATE not in asset.index or EVENT_DATE not in market.index:
            continue
        prev_idx = asset.index[asset.index.get_loc(EVENT_DATE) - 1]
        if prev_idx not in market.index:
            continue

        asset_event = asset.loc[EVENT_DATE]
        asset_prev = asset.loc[prev_idx]
        market_event = market.loc[EVENT_DATE]
        market_prev = market.loc[prev_idx]
        if isinstance(asset_event, pd.Series):
            asset_event = asset_event.iloc[0]
        if isinstance(asset_prev, pd.Series):
            asset_prev = asset_prev.iloc[0]
        if isinstance(market_event, pd.Series):
            market_event = market_event.iloc[0]
        if isinstance(market_prev, pd.Series):
            market_prev = market_prev.iloc[0]
        asset_ret = float(asset_event / asset_prev - 1.0)
        market_ret = float(market_event / market_prev - 1.0)
        abnormal = asset_ret - beta * market_ret

        # Residual distribution for z-score
        residuals = asset_returns[asset_returns.index < EVENT_DATE] - beta * market_returns[market_returns.index < EVENT_DATE]
        if isinstance(residuals, pd.DataFrame):
            residuals = residuals.iloc[:, 0]
        resid_std = float(residuals.std())
        z_score = abnormal / resid_std if resid_std > 0 else np.nan

        rows.append({
            "ticker": ticker,
            "beta": beta,
            "event_return": asset_ret,
            "abnormal_return": abnormal,
            "z_score": z_score
        })

    if not rows:
        logger.warning("No sector results.")
        return

    df = pd.DataFrame(rows).sort_values("abnormal_return", ascending=False)
    out_csv = os.path.join(OUTPUT_DIR, "sector_spdr_check.csv")
    df.to_csv(out_csv, index=False)
    logger.info(f"Saved sector check to {out_csv}")

    plt.figure(figsize=(9, 5))
    plt.bar(df["ticker"], df["abnormal_return"] * 100, color="#1f77b4")
    plt.axhline(0, color="gray", linewidth=0.8)
    plt.ylabel("Abnormal Return (%)")
    plt.title("Sector SPDR Abnormal Returns (Jan 2, 2026)")
    plt.tight_layout()

    out_png = os.path.join(OUTPUT_DIR, "Sector_SPDR_Check.png")
    plt.savefig(out_png)
    logger.info(f"Saved sector chart to {out_png}")

if __name__ == "__main__":
    run_sector_check()
