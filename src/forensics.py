import pandas as pd
import numpy as np
import os
import glob
import logging
from datetime import datetime, timedelta
import models


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw_parquet')

def load_ticker_data(ticker: str, target_date_str: str = None) -> pd.DataFrame:
    """
    Load ticker data from parquet
    If target_date_str is provided, selects the file covering that date.
    """
    safe_ticker = ticker.replace("=", "")
    # Explicitly look for 5m data first for intraday analysis
    pattern = os.path.join(RAW_DIR, f"{safe_ticker}_*_5m.parquet")
    files = glob.glob(pattern)
    
    if not files:
        # Fallback to any if no specific 5m found
        pattern = os.path.join(RAW_DIR, f"{safe_ticker}_*.parquet")
        files = glob.glob(pattern)
        
    if not files:
        raise FileNotFoundError(f"No data found for {ticker}")
    
    selected_file = None
    
    if target_date_str:
        target_dt = pd.Timestamp(target_date_str).date()
        for f in files:
            # Parse filename: Ticker_Start_End_Interval.parquet
            try:
                # Expecting format like: XLE_2026-01-02_2026-01-06_5m.parquet
                parts = os.path.basename(f).split('_')
                if len(parts) >= 3:
                    start_str = parts[1]
                    end_str = parts[2]
                    
                    # specific fix for end_date parsing if it contains .parquet
                    # usually it's date_date_interval.parquet
                    
                    file_start = pd.Timestamp(start_str).date()
                    file_end = pd.Timestamp(end_str).date()
                    
                    if file_start <= target_dt <= file_end:
                        selected_file = f
                        break
            except Exception as e:
                logger.warning(f"Failed to parse filename {f}: {e}")
                continue
    
    if not selected_file:
        # Load the most recent file if multiple
        files.sort(key=os.path.getmtime, reverse=True)
        selected_file = files[0]
        
    logger.info(f"Loading {ticker} from {os.path.basename(selected_file)}")
    df = pd.read_parquet(selected_file)
    
    # Filter by date if provided (assuming timezone naive or compatible)
    if target_date_str:
        # We don't filter rows here rigidly, just returning the correct file dataset
        pass
        
    return df

def calculate_leak_spread(xle_df: pd.DataFrame, uso_df: pd.DataFrame, date_str: str):
    """
    Calculate the cumulative return spread between XLE and Oil Futures (CL=F) for a specific date.
    """
    
    # Try flexible string slicing if simple .loc fails
    try:
        xle_day = xle_df.loc[date_str].copy()
        uso_day = uso_df.loc[date_str].copy()
    except KeyError:
        # Fallback: Filter by string matching on date component
        # This handles cases where index is tz-aware but we pass naive string
        try:
             target_date = pd.Timestamp(date_str).date()
             xle_day = xle_df[xle_df.index.date == target_date].copy()
             uso_day = uso_df[uso_df.index.date == target_date].copy()
             if xle_day.empty or uso_day.empty:
                 raise KeyError("Empty after date filter")
        except Exception as e:
             logger.error(f"Fallback filter failed: {e}")
             return None

    if xle_day.empty or uso_day.empty:
        logger.warning("One of the dataframes is empty.")
        return None

    # Filter for RTH (09:30 - 16:00 EST)
    if xle_day.index.tz is None:
        xle_day.index = xle_day.index.tz_localize('America/New_York')
    else:
        xle_day.index = xle_day.index.tz_convert('America/New_York')
        
    if uso_day.index.tz is None:
        uso_day.index = uso_day.index.tz_localize('America/New_York')
    else:
        uso_day.index = uso_day.index.tz_convert('America/New_York')

    xle_day = xle_day.between_time('09:30', '16:00')
    uso_day = uso_day.between_time('09:30', '16:00')

    if xle_day.empty or uso_day.empty:
        logger.warning("Dataframes empty after RTH filter.")
        return None

    # Align timestamps
    common_index = xle_day.index.union(uso_day.index).sort_values()
    xle_day = xle_day.reindex(common_index).ffill() 
    uso_day = uso_day.reindex(common_index).ffill()
    
    # Handle potential MultiIndex columns (Price, Ticker) by selecting first column
    xle_close = xle_day['Close']
    if isinstance(xle_close, pd.DataFrame):
        xle_close = xle_close.iloc[:, 0]
        
    uso_close = uso_day['Close']
    if isinstance(uso_close, pd.DataFrame):
        uso_close = uso_close.iloc[:, 0]

    # Normalize to Open
    xle_start = xle_close.iloc[0]
    uso_start = uso_close.iloc[0]
    
    xle_norm = (xle_close / xle_start)
    uso_norm = (uso_close / uso_start)
    
    spread = xle_norm - uso_norm
    
    return spread, xle_norm, uso_norm

def run_forensic_analysis():
    target_date = "2026-01-02"
    logger.info(f"Running forensic analysis for {target_date}...")
    
    # 1: Intraday Leak Detection (XLE vs Oil Futures)
    try:
        xle = load_ticker_data("XLE", target_date)
        uso = load_ticker_data("CL=F", target_date) # Using Futures as proxy
    except FileNotFoundError as e:
        logger.error(e)
        return

    # 1. calc Spread
    results = calculate_leak_spread(xle, uso, target_date)
    max_vol_val = 0 # Default
    
    if results:
        spread, xle_growth, uso_growth = results
        
        # ensure series
        if isinstance(spread, pd.DataFrame):
            spread = spread.iloc[:, 0]
            
        # Identify Key moments
        max_spread = spread.max()
        max_spread_time = spread.idxmax()
        
        # Ensure scalar max
        if isinstance(max_spread, pd.Series):
            max_spread = max_spread.iloc[0]
            
        print(f"\n FORENSIC REPORT: {target_date}")
        print(f"Dataset: XLE (Sector) vs CL=F (Crude Futures)")
        print(f"Max Decoupling Spread: +{max_spread:.2%} (XLE outperformance)")
        
        # EST
        if max_spread_time.tzinfo is not None:
            max_spread_time_est = max_spread_time.tz_convert('America/New_York')
        else:
            max_spread_time_est = max_spread_time
            
        print(f"Time of Peak Spread:   {max_spread_time_est.strftime('%H:%M EST')}")
        
        # Volume Spike Analysis
        xle_day = xle.loc[target_date]
        xle_vol = xle_day['Volume']
        if isinstance(xle_vol, pd.DataFrame):
            xle_vol = xle_vol.iloc[:, 0]
            
        max_vol_time = xle_vol.idxmax()
        max_vol_val = xle_vol.max()
        
        if max_vol_time.tzinfo is not None:
            max_vol_time_est = max_vol_time.tz_convert('America/New_York')
        else:
            max_vol_time_est = max_vol_time
        
        print(f"Max Volume Spike:      {max_vol_val:,.0f} shares at {max_vol_time_est.strftime('%H:%M EST')}")

    # Statistics Verification (Correlation & Spread Z-Score)
    print(f"\n STATISTICS VERIFICATION")
    try:
        # Load 30-day Intraday Baseline (Dec 2025)
        # We use this for Correlation and Spread Distribution
        xle_base = pd.read_parquet(os.path.join(RAW_DIR, "XLE_2025-12-01_2026-01-01_5m.parquet"))
        oil_base = pd.read_parquet(os.path.join(RAW_DIR, "CLF_2025-12-01_2026-01-01_5m.parquet"))
        
        # Align
        if xle_base.index.tz is None: xle_base.index = xle_base.index.tz_localize('America/New_York')
        else: xle_base.index = xle_base.index.tz_convert('America/New_York')
            
        if oil_base.index.tz is None: oil_base.index = oil_base.index.tz_localize('America/New_York')
        else: oil_base.index = oil_base.index.tz_convert('America/New_York')
            
        # RTH Only
        xle_base = xle_base.between_time("09:30", "16:00")
        oil_base = oil_base.between_time("09:30", "16:00")
        
        common = xle_base.index.intersection(oil_base.index)
        xle_base = xle_base.loc[common]
        oil_base = oil_base.loc[common]
        
        xle_ret = xle_base['Close'].pct_change().dropna()
        oil_ret = oil_base['Close'].pct_change().dropna()
        if isinstance(xle_ret, pd.DataFrame): xle_ret = xle_ret.iloc[:, 0]
        if isinstance(oil_ret, pd.DataFrame): oil_ret = oil_ret.iloc[:, 0]
            
        corr = xle_ret.corr(oil_ret)
        print(f"Baseline Correlation (rho): {corr:.2f} (Verified)")
        
        # Spread Z-Score
        xle_d = pd.read_parquet(os.path.join(RAW_DIR, "XLE_2025-10-01_2026-01-03_1d.parquet"))
        oil_d = pd.read_parquet(os.path.join(RAW_DIR, "CLF_2025-10-01_2026-01-03_1d.parquet"))
        
        # Align Daily
        common_d = xle_d.index.intersection(oil_d.index)
        xle_d = xle_d.loc[common_d]['Close']
        oil_d = oil_d.loc[common_d]['Close']
        
        if isinstance(xle_d, pd.DataFrame): xle_d = xle_d.iloc[:, 0]
        if isinstance(oil_d, pd.DataFrame): oil_d = oil_d.iloc[:, 0]
            
        daily_spreads = xle_d.pct_change() - oil_d.pct_change()
        spread_mean = daily_spreads.mean()
        spread_std = daily_spreads.std()
        
        # Use the calculated max spread from above
        event_spread = max_spread
        

        z_score_spread = (event_spread - spread_mean) / spread_std
        print(f"Daily Spread Mean: {spread_mean:.4f}")
        print(f"Daily Spread Std:  {spread_std:.4f}")
        print(f"Event Spread (Jan 2): {event_spread:.4f}")
        print(f"Spread Z-Score:    {z_score_spread:.2f}")
        
        print(f"NOTE: Spread is {z_score_spread:.2f}")

    except Exception as e:
        logger.error(f"Stats check failed: {e}")

    # 2: Beta Check (Supply Chain Hypothesis)
    print(f"\n HYPOTHESIS TEST: Supply Chain Leak (HAL/SLB)")
    print("Testing if HAL/SLB moves were idiosyncratic (leak) or systematic (beta).")
    
    contractors = ["HAL", "SLB"]
    for ticker in contractors:
        res = models.run_beta_analysis(ticker, "XLE")
        if res:
            print(f"\nTicker: {ticker}")
            print(f"  Return:           {res['event_return']:.2%}")
            print(f"  Beta (vs XLE):    {res['beta']:.2f}")
            print(f"  Exp. Return:      {res['expected_return']:.2%}")
            print(f"  Abnormal Return:  {res['abnormal_return']:.2%}")
            print(f"  Z-Score:          {res['z_score']:.2f}")
            
            if abs(res['z_score']) > 1.96:
                 print("CONCLUSION: Statistically Significant Abnormal Return (POSSIBLE LEAK)")
            else:
                 print("CONCLUSION: Movement explained by Sector Beta (NO LEAK)")

    # 3: Hierarchy of Information (ITA vs shipping basket)
    print(f"\n HYPOTHESIS TEST: Hierarchy of Information")
    print("Testing 'Kinetic Action' (Defense) vs 'Supply Disruption' (Shipping) hypothesis.")
    
    # Defense
    ita_res = models.run_beta_analysis("ITA", "SPY")
    
    # Shipping Basket
    shipping_tickers = ["FRO", "NAT", "STNG"]
    shipping_results = []
    
    print("\n Defense Sector")
    if ita_res:
         print(f"ITA Return: {ita_res['event_return']:.2%} (Beta: {ita_res['beta']:.2f})")

    print("\n Shipping Sector (Basket)")
    total_shipping_return = 0
    valid_shipping_count = 0
    
    for ticker in shipping_tickers:
        res = models.run_beta_analysis(ticker, "SPY")
        if res:
            print(f"{ticker} Return: {res['event_return']:.2%} (Beta: {res['beta']:.2f})")
            total_shipping_return += res['event_return']
            valid_shipping_count += 1
            shipping_results.append(res)
    
    if valid_shipping_count > 0 and ita_res:
        avg_shipping_return = total_shipping_return / valid_shipping_count
        print(f"\nAverage Shipping Return: {avg_shipping_return:.2%}")
        
        if ita_res['event_return'] > 0.02 and avg_shipping_return < 0:
            print("PATTERN CONFIRMED: Defense UP, Shipping Basket DOWN.")
            print("IMPLICATION: Market priced in 'Surgical Strike' (Kinetic) not 'Protracted War' (Supply Chain).")
        else:
            print("PATTERN NOT CONFIRMED.")

    # 4: Volume Test
    print(f"\n HYPOTHESIS TEST: Volume Spike Test")
    print("Testing if 15:55 EST volume was abnormal relative to typical Market-On-Close (MOC) activity.")
    
    try:
        # Load Baseline Data (Dec 1 - Jan 1)
        baseline_file = os.path.join(RAW_DIR, "XLE_2025-12-01_2026-01-01_5m.parquet")
        if os.path.exists(baseline_file):
            df_base = pd.read_parquet(baseline_file)
            
            # Ensure TZ alignment
            if df_base.index.tz is None:
                df_base.index = df_base.index.tz_localize('America/New_York')
            else:
                df_base.index = df_base.index.tz_convert('America/New_York')
            
            # Filter for 15:55 bars only
            # The timestamp for the 5m bar ending at 16:00 is usually 15:55
            moc_volumes = df_base.between_time('15:55', '15:55')['Volume']
            
            if not moc_volumes.empty:
                mean_vol = moc_volumes.mean()
                std_vol = moc_volumes.std()
                
                # Get Event Volume (Jan 2 15:55)
                event_vol = float(max_vol_val)
                mean_vol = float(mean_vol)
                std_vol = float(std_vol)
                
                z_score_vol = (event_vol - mean_vol) / std_vol
                
                print(f"Baseline MOC Mean Volume: {mean_vol:,.0f}")
                print(f"Baseline MOC Std Dev:     {std_vol:,.0f}")
                print(f"Event Volume (Jan 2):     {event_vol:,.0f}")
                print(f"Volume Z-Score vs Hist:   {z_score_vol:.2f}")
                
                if z_score_vol > 3.0:
                    print("CONCLUSION: Volume Spike was STATISTICALLY SIGNIFICANT (Abnormal).")
                    print("RESULT: Refutes 'Standard MOC' critique.")
                else:
                    print("CONCLUSION: Volume Spike was consistent with historical MOC flows.")
                    print("RESULT: Supports 'Standard MOC' critique.")
            else:
                print("Error: No 15:55 bars found in baseline data.")
        else:
            print("Error: Baseline data file not found.")
            
    except Exception as e:
        logger.error(f"Volume test failed: {e}")

if __name__ == "__main__":
    run_forensic_analysis()
