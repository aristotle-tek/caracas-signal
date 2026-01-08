import pandas as pd
import numpy as np
import statsmodels.api as sm
import os
import glob
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_DIR = "data/raw_parquet"

def load_5m_data(ticker, date_filter=None):
    safe_ticker = ticker.replace("=", "")
    pattern = os.path.join(RAW_DIR, f"{safe_ticker}_*_5m.parquet")
    files = glob.glob(pattern)
    
    combined_df = pd.DataFrame()
    print(f"Loading {ticker} files: {[os.path.basename(f) for f in files]}")
    for f in files:
        try:
            df = pd.read_parquet(f)
            combined_df = pd.concat([combined_df, df])
        except Exception as e:
            pass
            
    if combined_df.empty:
        raise FileNotFoundError(f"No data for {ticker}")
        
    combined_df = combined_df[~combined_df.index.duplicated(keep='first')].sort_index()
    
    # TZ Normalize
    if combined_df.index.tz is None:
        combined_df.index = combined_df.index.tz_localize('America/New_York')
    else:
        combined_df.index = combined_df.index.tz_convert('America/New_York')
        
    # RTH Filter
    combined_df = combined_df.between_time("09:30", "16:00")
    
    if date_filter:
        combined_df = combined_df[combined_df.index.strftime('%Y-%m-%d').isin(date_filter)]
        
    return combined_df

def run_factor_model():
    print("--- INTRADAY FACTOR MODEL (XLE ~ SPY + OIL) ---")
    
    # Baseline: Dec 1, 2025 to Dec 31, 2025
    # Event: Jan 2, 2026
    
    try:
        xle = load_5m_data("XLE")
        spy = load_5m_data("SPY")
        oil = load_5m_data("CL=F")
        
        # Align
        common_idx = xle.index.intersection(spy.index).intersection(oil.index)
        xle = xle.loc[common_idx]
        spy = spy.loc[common_idx]
        oil = oil.loc[common_idx]
        
        xle_ret = xle['Close'].pct_change().dropna()
        spy_ret = spy['Close'].pct_change().dropna()
        oil_ret = oil['Close'].pct_change().dropna()
        
        if isinstance(xle_ret, pd.DataFrame): xle_ret = xle_ret.iloc[:, 0]
        if isinstance(spy_ret, pd.DataFrame): spy_ret = spy_ret.iloc[:, 0]
        if isinstance(oil_ret, pd.DataFrame): oil_ret = oil_ret.iloc[:, 0]
        
        # Split train (baseline) / test (event)
        # event is 2026-01-02
        train_mask = xle_ret.index < "2026-01-01"
        test_mask = xle_ret.index.date == pd.Timestamp("2026-01-02").date()
        
        # training data
        print(f"Total Returns: {len(xle_ret)}")
        print(f"Training Mask Sum: {train_mask.sum()}")
        
        spy_train = spy_ret[train_mask]
        oil_train = oil_ret[train_mask]
        print(f"SPY Train type: {type(spy_train)}")
        print(f"Oil Train type: {type(oil_train)}")
        print(f"SPY Train head: {spy_train.head()}")
        
        Y_train = xle_ret[train_mask]
        X_train = pd.DataFrame({'SPY': spy_train, 'Oil': oil_train})
        X_train = sm.add_constant(X_train)
        
        if len(Y_train) < 100:
            print("Error: Insufficient training data.")
            return

        model = sm.OLS(Y_train, X_train).fit()
        
        # calc training resids Std Dev
        train_residuals = model.resid
        resid_std = train_residuals.std()
        
        print("\nModel Summary (Baseline: Dec 2025):")
        print(f"Alpha: {model.params['const']:.6f}")
        print(f"Beta_SPY: {model.params['SPY']:.4f}")
        print(f"Beta_Oil: {model.params['Oil']:.4f}")
        print(f"R-squared: {model.rsquared:.4f}")
        print(f"Residual Std Dev (5m): {resid_std:.6f}")
        
        # Test data (Jan 2)
        Y_test = xle_ret[test_mask]
        X_test = pd.DataFrame({'SPY': spy_ret[test_mask], 'Oil': oil_ret[test_mask]})
        X_test = sm.add_constant(X_test, has_constant='add')
        
        predicted_xle = model.predict(X_test)
        residuals = Y_test - predicted_xle
        
        # Cumulative Abnormal Return (CAR)
        cum_residuals = (1 + residuals).cumprod() - 1
        total_car = cum_residuals.iloc[-1]
        
        # To get Z-score of CAR, we need std of CAR.
        # Approx: Std(CAR) = sqrt(N) * Std(resid) assuming i.i.d.
        n_bars = len(residuals)
        car_std = np.sqrt(n_bars) * resid_std
        z_score_car = total_car / car_std
        
        print("\n--- Event Day Analysis (Jan 2, 2026) ---")
        print(f"Total Cumulative Abnormal Return (CAR): {total_car:.2%}")
        print(f"Expected CAR Std (approx): {car_std:.2%}")
        print(f"CAR Z-Score: {z_score_car:.2f}")
        
        # Check specific time window (After 14:55)
        leak_start = pd.Timestamp("2026-01-02 14:55:00", tz='America/New_York')
        post_leak = cum_residuals[cum_residuals.index >= leak_start]
        
        if not post_leak.empty:
            print(f"CAR at 14:55: {cum_residuals[cum_residuals.index < leak_start].iloc[-1]:.2%}")
            print(f"CAR at Close: {cum_residuals.iloc[-1]:.2%}")
            diff = cum_residuals.iloc[-1] - cum_residuals[cum_residuals.index < leak_start].iloc[-1]
            print(f"Unexplained Surge (Post-Leak): {diff:.2%}")
            
            if diff > 0.01:
                print(">>> CONCLUSION: Significant Unexplained Alpha (>1%).")
                print(">>> RESULT: 'Risk-On' and 'Oil Beta' CANNOT explain the move.")
            else:
                print(">>> CONCLUSION: Move explained by factors.")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_factor_model()
