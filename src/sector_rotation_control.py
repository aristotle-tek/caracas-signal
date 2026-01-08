import yfinance as yf
import pandas as pd
import numpy as np

def check_january_effect():
    tickers = ["XLF", "SMH", "QQQ", "SPY"] # Benchmarks
    # Note: XLE spread is known (+1.94%)
    
    start = "2026-01-02"
    end = "2026-01-06"
    
    print("Downloading Sector Data for Jan 2, 2026...")
    data = {}
    for t in tickers:
        try:
            df = yf.download(t, start=start, end=end, interval="5m", progress=False, auto_adjust=True)
            # Filter RTH Jan 2
            df = df.between_time("09:30", "16:00")
            day1 = df[df.index.date == pd.Timestamp("2026-01-02").date()]
            data[t] = day1['Close']
        except Exception as e:
            print(f"Failed {t}: {e}")
            
    if not data:
        return

    # calc cumulative Returns
    returns = {}
    for t, price_series in data.items():
        if price_series.empty: continue
        # Normalize to open
        start_price = float(price_series.iloc[0])
        end_price = float(price_series.iloc[-1])
        ret = (end_price / start_price) - 1
        returns[t] = ret
        print(f"{t} Return: {ret:.2%}")

    # calc Spreads
    # XLF vs SPY
    if "XLF" in returns and "SPY" in returns:
        xlf_spread = returns["XLF"] - returns["SPY"]
        print(f"\nFinancials Spread (XLF - SPY): {xlf_spread:.2%}")
    
    # SMH vs QQQ
    if "SMH" in returns and "QQQ" in returns:
        smh_spread = returns["SMH"] - returns["QQQ"]
        print(f"Semis Spread (SMH - QQQ): {smh_spread:.2%}")
        
    print(f"Energy Spread (XLE - Oil): +1.94% (Benchmark)")

if __name__ == "__main__":
    check_january_effect()
