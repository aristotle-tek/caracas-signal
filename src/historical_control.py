import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import os
import logging
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = "data/raw_parquet"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

EVENTS = {
    "Libya (2011)": {
        "date": "2011-03-19", # NATO intervention
        "desc": "War Risk / Supply Disruption"
    },
    "Syria Strike (2017)": {
        "date": "2017-04-07", # US Tomahawk strike
        "desc": "Surgical Strike"
    },
    "Oman (2019)": {
        "date": "2019-06-13",
        "desc": "Tanker attacks"
    },
    "Abqaiq (2019)": {
        "date": "2019-09-16",
        "desc": "Supply Destruction"
    },
    "Soleimani (2020)": {
        "date": "2020-01-03",
        "desc": "Escalation Risk"
    },
    "Suez Blockage (2021)": {
        "date": "2021-03-24", # Ever Given stuck
        "desc": "Shipping Logistics Shock (Control)"
    },
    "Ukraine (2022)": {
        "date": "2022-02-24",
        "desc": "Invasion / War Risk"
    },
    "Israel-Hamas (2023)": {
        "date": "2023-10-09", # Market reaction Monday after Oct 7
        "desc": "Regional War Risk"
    },
    "Caracas (2026)": {
        "date": "2026-01-02",
        "desc": "Operation Stabilize"
    }
}

DEFENSE_TICKER = "ITA"
SHIPPING_BASKET = ["FRO", "NAT", "STNG"]

def get_return(ticker, date_str):
    """
    Get (T-1 to T+1) return for a ticker around an event.
    """
    target_date = pd.Timestamp(date_str)
    # Fetch wider window to ensure we catch the trading day
    start_date = target_date - pd.Timedelta(days=5)
    end_date = target_date + pd.Timedelta(days=5)
    
    try:
        df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=True)
        if df.empty:
            return None
            
        # Ensure we find the specific event date or next trading day
        # If event is Sat/Sun, we want Monday close vs Friday close?
        # Or Monday Open vs Monday Close?
        # Standard event study: Close(T) / Close(T-1) - 1.
        # If T is non-trading, use next trading day.
        
        idx_loc = df.index.searchsorted(target_date)
        if idx_loc >= len(df):
            return None
            
        # If target_date is exact match or after, idx_loc points to it (or next)
        # We want return of that day relative to prev closing.
        
        # Robust calculation:
        # P_event = Close of the event day (or first trading day after)
        # P_prev = Close of the previous trading day
        
        p_event = float(df['Close'].iloc[idx_loc])
        
        if idx_loc == 0:
            # Cannot calc return if it's the first day in window
            return None
            
        p_prev = float(df['Close'].iloc[idx_loc - 1])
        
        return (p_event / p_prev) - 1.0
        
    except Exception as e:
        logger.warning(f"Error for {ticker} at {date_str}: {e}")
        return None

def run_historical_study():
    print(f"\n{'='*90}")
    print(f"{'Event':<22} | {'Date':<10} | {'ITA (Def)':<10} | {'Ship Basket':<12} | {'Correlation / Type'}")
    print(f"{'-'*90}")
    
    results = []
    
    # Sort by date
    sorted_events = sorted(EVENTS.items(), key=lambda x: x[1]['date'])
    
    for name, info in sorted_events:
        dt = info['date']
        
        # defense Return
        ita_ret = get_return(DEFENSE_TICKER, dt)
        
        # shipping Basket Return
        basket_rets = []
        for t in SHIPPING_BASKET:
            r = get_return(t, dt)
            if r is not None:
                basket_rets.append(r)
        
        if ita_ret is not None and basket_rets:
            avg_ship_ret = np.mean(basket_rets)
            
            ita_pct = ita_ret * 100
            ship_pct = avg_ship_ret * 100
            
            signal = ""
            if ita_pct > 0.5 and ship_pct > 0.5:
                signal = "War Risk (Positive)"
            elif ita_pct > 0.5 and ship_pct < -0.5:
                signal = "Stabilization (Negative)"
            elif ita_pct < -0.5 and ship_pct > 0.5:
                signal = "De-escalation (Negative)"
            elif abs(ita_pct) < 0.5 and abs(ship_pct) > 1.0:
                signal = "Pure Shipping Shock"
            else:
                signal = "Mixed / Noise"
            
            print(f"{name:<22} | {dt:<10} | {ita_pct:>9.2f}% | {ship_pct:>11.2f}% | {signal}")
        else:
            print(f"{name:<22} | {dt:<10} | {'N/A':>10} | {'N/A':>12} | Data Missing")
            
    print(f"{'='*90}\n")

if __name__ == "__main__":
    run_historical_study()
