import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
import glob
import logging
import yfinance as yf
import plot_style 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw_parquet')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'out')

COL_DEFENSE = "#1f77b4" # Blue
COL_SHIPPING = "#ff7f0e" # Orange
COL_HIGHLIGHT = "#d62728" # Red

# Historical Events
EVENTS = {
    "Libya (2011)": "2011-03-19",
    "Syria (2017)": "2017-04-07",
    "Oman (2019)": "2019-06-13",
    "Abqaiq (2019)": "2019-09-16",
    "Soleimani (2020)": "2020-01-03",
    "Suez (2021)": "2021-03-24",
    "Ukraine (2022)": "2022-02-24",
    "Israel-Hamas (2023)": "2023-10-09",
    "Caracas (2026)": "2026-01-02"
}

def load_ticker_data(ticker: str, target_date_str: str = "2026-01-02") -> pd.DataFrame:
    # Existing load logic for Forensic Chart
    safe_ticker = ticker.replace("=", "")
    pattern_5m = os.path.join(RAW_DIR, f"{safe_ticker}_*_5m.parquet")
    files_5m = glob.glob(pattern_5m)
    
    files = files_5m if files_5m else glob.glob(os.path.join(RAW_DIR, f"{safe_ticker}_*.parquet"))
    
    if not files:
        raise FileNotFoundError(f"No data for {ticker}")

    selected_file = None
    target_dt = pd.Timestamp(target_date_str).date()
    
    for f in files:
        try:
            parts = os.path.basename(f).split('_')
            if len(parts) >= 3:
                start_str = parts[1]
                end_str = parts[2]
                file_start = pd.Timestamp(start_str).date()
                file_end = pd.Timestamp(end_str).date()
                if file_start <= target_dt <= file_end:
                    selected_file = f
                    break
        except:
            continue
            
    if not selected_file:
        raise FileNotFoundError(f"No parquet file found for {ticker} covering date {target_date_str}")

    logger.info(f"Loading {ticker} from {os.path.basename(selected_file)}")
    return pd.read_parquet(selected_file)

def get_historical_data(ticker, event_date, window=2):
    """Fetch T-window to T+window daily data."""
    target = pd.Timestamp(event_date)
    start = target - pd.Timedelta(days=window*2 + 5) # Buffer
    end = target + pd.Timedelta(days=window*2 + 5)
    
    try:
        df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if df.empty: return None
        
        # Center on event
        idx_loc = df.index.searchsorted(target)
        if idx_loc >= len(df): return None
        
        # Determine actual event date in trading calendar
        # If event is non-trading, searchsorted returns next trading day.
        # This is effectively T=0.
        
        # Slice window
        start_idx = max(0, idx_loc - window)
        end_idx = min(len(df), idx_loc + window + 1)
        
        sliced = df.iloc[start_idx:end_idx].copy()
        
        # Normalize to T-1
        # T=0 is at row [idx_loc - start_idx]
        zero_pos = idx_loc - start_idx
        
        if zero_pos > 0:
            base_price = float(sliced['Close'].iloc[zero_pos - 1]) # T-1
        else:
            base_price = float(sliced['Close'].iloc[0]) # Fallback
            
        sliced['Norm'] = (sliced['Close'] / base_price - 1) * 100
        sliced['Day'] = range(len(sliced))
        sliced['Day'] = sliced['Day'] - zero_pos # Center X axis
        
        return sliced
        
    except Exception as e:
        logger.warning(f"Failed to fetch {ticker} for {event_date}: {e}")
        return None

def generate_forensic_chart():
    logger.info("Generating forensic chart...")
    plot_style.apply_plot_style()
    
    try:
        xle = load_ticker_data("XLE")
        oil_fut = load_ticker_data("CL=F")
    except FileNotFoundError as e:
        logger.error(e)
        return

    start_time = pd.Timestamp("2026-01-02 09:30:00", tz="America/New_York")
    end_time = pd.Timestamp("2026-01-05 16:00:00", tz="America/New_York")
    
    if xle.index.tz is None: xle.index = xle.index.tz_localize("America/New_York")
    else: xle.index = xle.index.tz_convert("America/New_York")
        
    if oil_fut.index.tz is None: oil_fut.index = oil_fut.index.tz_localize("America/New_York")
    else: oil_fut.index = oil_fut.index.tz_convert("America/New_York")

    xle_plot = xle[(xle.index >= start_time) & (xle.index <= end_time)]
    oil_plot = oil_fut[(oil_fut.index >= start_time) & (oil_fut.index <= end_time)]

    xle_close = xle_plot['Close']
    if isinstance(xle_close, pd.DataFrame): xle_close = xle_close.iloc[:, 0]
    oil_close = oil_plot['Close']
    if isinstance(oil_close, pd.DataFrame): oil_close = oil_close.iloc[:, 0]
        
    base_price_xle = xle_close.iloc[0]
    friday_open_mask = oil_close.index >= start_time
    if friday_open_mask.any(): base_price_oil = oil_close.loc[friday_open_mask].iloc[0]
    else: base_price_oil = oil_close.iloc[0]
    
    xle_norm = (xle_close / base_price_xle - 1) * 100
    oil_norm = (oil_close / base_price_oil - 1) * 100

    plt.figure(figsize=(14, 7))
    plt.plot(xle_norm.index, xle_norm, label='Energy Stocks (XLE)', color=COL_DEFENSE, linewidth=2)
    plt.plot(oil_norm.index, oil_norm, label='Oil Futures (CL=F)', color=COL_SHIPPING, linewidth=1.5, linestyle='--')

    sunday_mask = (oil_norm.index.day == 4)
    if sunday_mask.any():
        plt.scatter(oil_norm[sunday_mask].index, oil_norm[sunday_mask], color=COL_HIGHLIGHT, s=15, label='Sunday Trading')

    plt.title("Forensic Analysis: XLE vs Oil Futures (Jan 2 - Jan 5, 2026)", fontsize=14)
    plt.ylabel("Cumulative Return (%)")
    plt.xlabel("Date / Time (EST)")
    
    leak_time = pd.Timestamp("2026-01-02 14:55:00", tz="America/New_York")
    plt.axvline(leak_time, color='black', linestyle=':', label='Info Leak (14:55)')
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%a %H:%M'))
    plt.legend(loc='upper left')
    plt.tight_layout()

    output_path = os.path.join(OUTPUT_DIR, "Forensic_Chart.png")
    plt.savefig(output_path)
    logger.info(f"Chart saved to {output_path}")

def generate_hierarchy_chart():
    logger.info("Generating hierarchy chart (ITA vs FRO)...")
    plot_style.apply_plot_style()
    
    try:
        ita = load_ticker_data("ITA") # defense
        fro = load_ticker_data("FRO") # shipping
    except FileNotFoundError as e:
        logger.error(e)
        return

    # Define timeframe: Jan 2, 2026 Trading Session
    start_time = pd.Timestamp("2026-01-02 09:30:00", tz="America/New_York")
    end_time = pd.Timestamp("2026-01-02 16:00:00", tz="America/New_York")
    
    # Ensure timezone alignment
    for df in [ita, fro]:
        if df.index.tz is None:
            df.index = df.index.tz_localize("America/New_York")
        else:
            df.index = df.index.tz_convert("America/New_York")

    # Filter
    ita_plot = ita[(ita.index >= start_time) & (ita.index <= end_time)]
    fro_plot = fro[(fro.index >= start_time) & (fro.index <= end_time)]

    # Normalize to Open
    ita_close = ita_plot['Close']
    if isinstance(ita_close, pd.DataFrame): ita_close = ita_close.iloc[:, 0]
    
    fro_close = fro_plot['Close']
    if isinstance(fro_close, pd.DataFrame): fro_close = fro_close.iloc[:, 0]
        
    ita_base = ita_close.iloc[0]
    fro_base = fro_close.iloc[0]
    
    ita_norm = (ita_close / ita_base - 1) * 100
    fro_norm = (fro_close / fro_base - 1) * 100

    # Plot
    plt.figure(figsize=(10, 6))

    plt.plot(ita_norm.index, ita_norm, label='Defense (ITA)', color=COL_DEFENSE, linewidth=2)
    plt.plot(fro_norm.index, fro_norm, label='Shipping (FRO)', color=COL_SHIPPING, linewidth=2)

    plt.title("Information Hierarchy: Defense vs Shipping (Jan 2, 2026)", fontsize=14)
    plt.ylabel("Intraday Return (%)")
    plt.xlabel("Time (EST)")
    
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.legend(loc='best')
    plt.tight_layout()

    output_path = os.path.join(OUTPUT_DIR, "Hierarchy_Chart.png")
    plt.savefig(output_path)
    logger.info(f"Chart saved to {output_path}")

def generate_historical_multipanel():
    logger.info("Generating historical multipanel time series...")
    plot_style.apply_plot_style()
    
    events_list = list(EVENTS.items())
    # Sort chronologically
    events_list.sort(key=lambda x: x[1])
    
    fig, axes = plt.subplots(3, 3, figsize=(15, 10), sharex=True, sharey=True)
    axes = axes.flatten()
    
    ship_tickers = ["FRO", "NAT", "STNG"]
    
    for i, (name, date) in enumerate(events_list):
        ax = axes[i]
        
        # 1. Defense (ITA)
        ita_df = get_historical_data("ITA", date)
        if ita_df is not None:
            ax.plot(ita_df['Day'], ita_df['Norm'], color=COL_DEFENSE, linewidth=2, label='Defense')
            
        # 2. Shipping Basket
        ship_curves = []
        for t in ship_tickers:
            df = get_historical_data(t, date)
            if df is not None:
                df = df.set_index('Day')['Norm']
                ship_curves.append(df)
        
        if ship_curves:
            # Concat and mean axis=1
            # Handle misaligned indices by outer join
            basket_df = pd.concat(ship_curves, axis=1).mean(axis=1)
            # Sort index just in case
            basket_df = basket_df.sort_index()
            ax.plot(basket_df.index, basket_df, color=COL_SHIPPING, linewidth=2, label='Shipping')
            
        ax.set_title(f"{name}", fontsize=10, weight='bold')
        ax.axvline(0, color='black', linestyle=':', linewidth=0.8)
        ax.axhline(0, color='gray', linewidth=0.5)
        
        if i == 0:
            ax.legend(loc='upper left', fontsize=8)
            
    fig.suptitle("Historical Control: Defense vs Shipping (T-2 to T+2)", fontsize=14, y=0.98)
    fig.text(0.5, 0.02, 'Trading Days Relative to Event (T=0)', ha='center')
    fig.text(0.02, 0.5, 'Cumulative Return (%)', va='center', rotation='vertical')
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.92)
    
    output_path = os.path.join(OUTPUT_DIR, "Historical_Control_Chart.png")
    plt.savefig(output_path)
    logger.info(f"Chart saved to {output_path}")

if __name__ == "__main__":
    generate_forensic_chart()
    generate_hierarchy_chart()
    generate_historical_multipanel()