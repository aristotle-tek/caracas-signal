import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import glob
import logging
import plot_style


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_DIR = "data/raw_parquet"
OUTPUT_DIR = "out"

def load_5m_data(ticker):
    safe_ticker = ticker.replace("=", "")
    pattern = os.path.join(RAW_DIR, f"{safe_ticker}_*_5m.parquet")
    files = glob.glob(pattern)
    combined_df = pd.DataFrame()
    for f in files:
        try:
            df = pd.read_parquet(f)
            combined_df = pd.concat([combined_df, df])
        except: pass
    if combined_df.empty: return None
    combined_df = combined_df[~combined_df.index.duplicated(keep='first')].sort_index()
    if combined_df.index.tz is None:
        combined_df.index = combined_df.index.tz_localize('America/New_York')
    else:
        combined_df.index = combined_df.index.tz_convert('America/New_York')
    return combined_df.between_time("09:30", "16:00")

def run_spread_placebo():
    logger.info("Running Spread Placebo Test (Distribution Analysis)...")
    plot_style.apply_plot_style()
    
    xle = load_5m_data("XLE")
    oil = load_5m_data("CL=F")
    
    if xle is None or oil is None:
        logger.error("Missing data")
        return

    # Align first
    common_idx = xle.index.intersection(oil.index)
    xle = xle.loc[common_idx]
    oil = oil.loc[common_idx]

    # Filter out the event day (Jan 2) from the baseline distribution
    baseline_mask = xle.index < "2026-01-01"
    xle_base = xle[baseline_mask]
    oil_base = oil[baseline_mask]
    
    unique_dates = np.unique(xle_base.index.date)
    
    daily_max_spreads = []
    
    print(f"\nAnalyzing {len(unique_dates)} baseline trading days...")
    
    for d in unique_dates:
        d_str = str(d)
        
        # Slice day
        xle_day = xle_base[xle_base.index.date == d]
        oil_day = oil_base[oil_base.index.date == d]
        
        if xle_day.empty or oil_day.empty: continue
        
        # align
        common = xle_day.index.intersection(oil_day.index)
        if len(common) < 30: continue # skip partial days
        
        xle_day = xle_day.loc[common]['Close']
        oil_day = oil_day.loc[common]['Close']
        
        # Handle df/series
        if isinstance(xle_day, pd.DataFrame): xle_day = xle_day.iloc[:, 0]
        if isinstance(oil_day, pd.DataFrame): oil_day = oil_day.iloc[:, 0]
            
        # Normalize to Open
        xle_norm = (xle_day / xle_day.iloc[0])
        oil_norm = (oil_day / oil_day.iloc[0])
        
        spread = xle_norm - oil_norm
        
        # Max deviation 
        max_s = spread.max()
        daily_max_spreads.append(max_s)
        
    daily_max_spreads = np.array(daily_max_spreads) * 100
    
    event_val = 1.94
    
    percentile = (daily_max_spreads < event_val).mean() * 100
    print(f"Jan 2 Spread: +{event_val:.2f}%")
    print(f"Baseline Max Spreads Mean: {daily_max_spreads.mean():.2f}%")
    print(f"Baseline Max Spreads Std:  {daily_max_spreads.std():.2f}%")
    print(f"Jan 2 Percentile: {percentile:.1f}%")
    

    plt.figure(figsize=(10, 6))
    
    plt.hist(daily_max_spreads, bins=10, color='lightgray', edgecolor='gray', alpha=0.7, label='Baseline Days (Nov-Dec 2025)')

    plt.axvline(event_val, color='#d62728', linewidth=2, linestyle='--', label=f'Jan 2 Event (+{event_val}%)')
    
    plt.title("Distribution of Daily Max Spreads (XLE - Oil)\n45-Day Intraday Baseline", fontsize=14)
    plt.xlabel("Max Intraday Spread (%)")
    plt.ylabel("Frequency (Days)")
    plt.legend()
    plt.tight_layout()
    
    output_path = os.path.join(OUTPUT_DIR, "Spread_Distribution_Chart.png")
    plt.savefig(output_path)
    logger.info(f"Chart saved to {output_path}")

if __name__ == "__main__":
    run_spread_placebo()
