import pandas as pd
import numpy as np
import os
import glob
import logging
from scipy import stats


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw_parquet')

def load_daily_data(ticker: str) -> pd.DataFrame:
    """Load daily data for a ticker from parquet."""
    safe_ticker = ticker.replace("=", "")
    pattern = os.path.join(RAW_DIR, f"{safe_ticker}_*_1d.parquet")
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"No daily data found for {ticker}")
        
    files.sort(key=os.path.getmtime, reverse=True)
    return pd.read_parquet(files[0])

def calculate_beta(asset_returns: pd.Series, market_returns: pd.Series) -> float:
    """Calculate Beta of asset relative to market."""
    # Align indices
    common_idx = asset_returns.index.intersection(market_returns.index)
    if len(common_idx) < 30:
        logger.warning("Insufficient overlapping data for Beta calculation")
        return np.nan
        
    y = asset_returns.loc[common_idx]
    x = market_returns.loc[common_idx]
    
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    return slope

def calculate_z_score(return_val: float, history: pd.Series) -> float:
    """Calculate Z-score of a single return value against historical distribution."""
    mu = history.mean()
    sigma = history.std()
    
    if sigma == 0:
        return np.nan
        
    return (return_val - mu) / sigma

def run_beta_analysis(target_ticker: str, market_ticker: str = "XLE"):
    """
    Run Beta/Z-score analysis for a target against a benchmark (Market/Sector).
    """
    try:
        target_df = load_daily_data(target_ticker)
        market_df = load_daily_data(market_ticker)
    except FileNotFoundError as e:
        logger.error(e)
        return None

    # Calculate Daily Returns
    # Handle potential df/ series issues
    target_close = target_df['Close']
    if isinstance(target_close, pd.DataFrame): target_close = target_close.iloc[:, 0]
        
    market_close = market_df['Close']
    if isinstance(market_close, pd.DataFrame): market_close = market_close.iloc[:, 0]

    target_rets = target_close.pct_change().dropna()
    market_rets = market_close.pct_change().dropna()

    event_date = pd.Timestamp("2026-01-02").date() # Normalize
    
    # Filter for history (pre-event)
    history_mask = target_rets.index.date < event_date
    
    target_hist = target_rets[history_mask]
    market_hist = market_rets[history_mask]
    
    beta = calculate_beta(target_hist, market_hist)
    
    # Event Day Analysis
    try:
        # flexible date matching
        # Check if event_date exists in the index dates
        event_mask = target_rets.index.date == event_date
        if not event_mask.any():
            logger.warning(f"Date {event_date} not found in {target_ticker} returns index.")
            logger.info(f"Available range: {target_rets.index[0].date()} to {target_rets.index[-1].date()}")
            return None
            
        target_event_ret = target_rets[event_mask].iloc[0]
        
        market_mask = market_rets.index.date == event_date
        if not market_mask.any():
             logger.warning(f"Date {event_date} not found in {market_ticker} returns index.")
             return None
             
        market_event_ret = market_rets[market_mask].iloc[0]
        
    except IndexError:
        logger.warning(f"IndexError accessing event data for {event_date}")
        return None

    # Expected Return = Alpha + Beta * Market_Return (Assuming Alpha=0 for short window)
    expected_ret = beta * market_event_ret
    abnormal_ret = target_event_ret - expected_ret
    
    residuals = target_hist - (beta * market_hist)
    residual_std = residuals.std()
    
    z_score = abnormal_ret / residual_std
    
    return {
        "ticker": target_ticker,
        "benchmark": market_ticker,
        "beta": beta,
        "event_return": target_event_ret,
        "expected_return": expected_ret,
        "abnormal_return": abnormal_ret,
        "z_score": z_score
    }
