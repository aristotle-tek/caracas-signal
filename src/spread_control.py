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

def _realized_vol(series):
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    returns = series.pct_change().dropna()
    return returns.std()

def run_spread_placebo():
    logger.info("Running Spread Placebo Test (Distribution Analysis)...")
    plot_style.apply_plot_style()
    
    xle = load_5m_data("XLE")
    oil = load_5m_data("CL=F")
    spy = load_5m_data("SPY")
    
    if xle is None or oil is None or spy is None:
        logger.error("Missing data")
        return

    # Align first
    common_idx = xle.index.intersection(oil.index)
    xle = xle.loc[common_idx]
    oil = oil.loc[common_idx]

    # Baseline = all available days before Jan 2, 2026
    baseline_mask = xle.index < "2026-01-02"
    xle_base = xle[baseline_mask]
    oil_base = oil[baseline_mask]
    spy_base = spy[baseline_mask]
    
    # Get list of unique dates
    unique_dates = np.unique(xle_base.index.date)
    
    daily_max_spreads = []
    daily_vols = []
    
    print(f"\nAnalyzing {len(unique_dates)} baseline trading days...")
    
    for d in unique_dates:
        d_str = str(d)
        
        # Slice day
        xle_day = xle_base[xle_base.index.date == d]
        oil_day = oil_base[oil_base.index.date == d]
        
        if xle_day.empty or oil_day.empty: continue
        
        # Align
        common = xle_day.index.intersection(oil_day.index).intersection(spy_base.index)
        if len(common) < 30: continue # Skip partial days
        
        xle_day = xle_day.loc[common]['Close']
        oil_day = oil_day.loc[common]['Close']
        spy_day = spy_base.loc[common]['Close']
        
        # Handle df/series
        if isinstance(xle_day, pd.DataFrame): xle_day = xle_day.iloc[:, 0]
        if isinstance(oil_day, pd.DataFrame): oil_day = oil_day.iloc[:, 0]
            
        # Normalize to Open
        xle_norm = (xle_day / xle_day.iloc[0])
        oil_norm = (oil_day / oil_day.iloc[0])
        
        spread = xle_norm - oil_norm
        
        # Max deviation (we focus on XLE > Oil)
        max_s = spread.max()
        daily_max_spreads.append(max_s)

        # Realized vol for matching
        daily_vols.append({
            "date": d,
            "spy_vol": _realized_vol(spy_day),
            "oil_vol": _realized_vol(oil_day)
        })
        
    daily_max_spreads = np.array(daily_max_spreads) * 100
    
    event_val = 1.94

    # Event day realized vol
    event_mask = xle.index.date == pd.Timestamp("2026-01-02").date()
    spy_event = spy[event_mask]['Close']
    oil_event = oil[event_mask]['Close']
    event_spy_vol = _realized_vol(spy_event)
    event_oil_vol = _realized_vol(oil_event)
    
    percentile = (daily_max_spreads < event_val).mean() * 100

    # Volatility-matched subset (within +/-20% of event vol for both SPY and Oil)
    vol_df = pd.DataFrame(daily_vols)
    if not vol_df.empty:
        lower_spy, upper_spy = event_spy_vol * 0.8, event_spy_vol * 1.2
        lower_oil, upper_oil = event_oil_vol * 0.8, event_oil_vol * 1.2
        matched_dates = vol_df[
            (vol_df["spy_vol"] >= lower_spy) & (vol_df["spy_vol"] <= upper_spy) &
            (vol_df["oil_vol"] >= lower_oil) & (vol_df["oil_vol"] <= upper_oil)
        ]["date"].tolist()
        matched_spreads = [
            s for d, s in zip(unique_dates, daily_max_spreads) if d in matched_dates
        ]
    else:
        matched_spreads = []
    print(f"Jan 2 Spread: +{event_val:.2f}%")
    print(f"Baseline Max Spreads Mean: {daily_max_spreads.mean():.2f}%")
    print(f"Baseline Max Spreads Std:  {daily_max_spreads.std():.2f}%")
    print(f"Jan 2 Percentile: {percentile:.1f}%")
    if matched_spreads:
        matched_arr = np.array(matched_spreads)
        matched_pct = (matched_arr < event_val).mean() * 100
        print(f"Vol-Matched Days: {len(matched_spreads)}")
        print(f"Vol-Matched Mean: {matched_arr.mean():.2f}%")
        print(f"Vol-Matched Std:  {matched_arr.std():.2f}%")
        print(f"Vol-Matched Percentile: {matched_pct:.1f}%")
    

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

    summary_path = os.path.join(OUTPUT_DIR, "spread_placebo_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Jan 2 Spread: {event_val:.2f}%\n")
        f.write(f"Baseline Days: {len(daily_max_spreads)}\n")
        f.write(f"Baseline Mean: {daily_max_spreads.mean():.2f}%\n")
        f.write(f"Baseline Std: {daily_max_spreads.std():.2f}%\n")
        f.write(f"Baseline Percentile: {percentile:.1f}%\n")
        if matched_spreads:
            matched_arr = np.array(matched_spreads)
            matched_pct = (matched_arr < event_val).mean() * 100
            f.write(f"Vol-Matched Days: {len(matched_spreads)}\n")
            f.write(f"Vol-Matched Mean: {matched_arr.mean():.2f}%\n")
            f.write(f"Vol-Matched Std: {matched_arr.std():.2f}%\n")
            f.write(f"Vol-Matched Percentile: {matched_pct:.1f}%\n")

if __name__ == "__main__":
    run_spread_placebo()
