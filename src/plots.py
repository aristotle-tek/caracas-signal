import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
from statsmodels.stats.diagnostic import breaks_cusumolsresid
import os
import glob
import logging
import matplotlib.dates as mdates
from plot_style import apply_plot_style


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw_parquet')
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'out')
os.makedirs(OUT_DIR, exist_ok=True)


apply_plot_style()

def load_data(ticker, interval, start_date=None, end_date=None):
    safe_ticker = ticker.replace("=", "").replace("^", "")
    pattern = os.path.join(RAW_DIR, f"{safe_ticker}_*_{interval}.parquet")
    files = glob.glob(pattern)
    
    if not files:
        return None
        
    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception:
            pass
            
    if not dfs:
        return None
        
    full_df = pd.concat(dfs).sort_index()
    full_df = full_df[~full_df.index.duplicated(keep='first')]
    
    # TZ-aware first (UTC from yfinance)
    if full_df.index.tz is None:
        full_df.index = full_df.index.tz_localize('UTC')
    
    # convert to EST
    full_df.index = full_df.index.tz_convert('America/New_York')
        
    if start_date:
        full_df = full_df[full_df.index >= pd.Timestamp(start_date).tz_localize('America/New_York')]
    if end_date:
        full_df = full_df[full_df.index <= pd.Timestamp(end_date).tz_localize('America/New_York')]
        
    return full_df

def get_common_returns(xle, spy, oil):
    # RTH Only (09:30 - 16:00 EST)
    xle = xle.between_time("09:30", "16:00")
    spy = spy.between_time("09:30", "16:00")
    oil = oil.between_time("09:30", "16:00")
    
    common = xle.index.intersection(spy.index).intersection(oil.index)
    
    p_xle = xle.loc[common]['Close']
    p_spy = spy.loc[common]['Close']
    p_oil = oil.loc[common]['Close']
    
    if isinstance(p_xle, pd.DataFrame): p_xle = p_xle.iloc[:, 0]
    if isinstance(p_spy, pd.DataFrame): p_spy = p_spy.iloc[:, 0]
    if isinstance(p_oil, pd.DataFrame): p_oil = p_oil.iloc[:, 0]
    
    # Log returns
    r_xle = np.log(p_xle / p_xle.shift(1)).dropna()
    r_spy = np.log(p_spy / p_spy.shift(1)).dropna()
    r_oil = np.log(p_oil / p_oil.shift(1)).dropna()
    
    # Re-align
    common_r = r_xle.index.intersection(r_spy.index).intersection(r_oil.index)
    return r_xle.loc[common_r], r_spy.loc[common_r], r_oil.loc[common_r]

def get_common_returns_multi(xle, spy, oil, brent):
    xle = xle.between_time("09:30", "16:00")
    spy = spy.between_time("09:30", "16:00")
    oil = oil.between_time("09:30", "16:00")
    brent = brent.between_time("09:30", "16:00")

    common = xle.index.intersection(spy.index).intersection(oil.index).intersection(brent.index)
    p_xle = xle.loc[common]['Close']
    p_spy = spy.loc[common]['Close']
    p_oil = oil.loc[common]['Close']
    p_brent = brent.loc[common]['Close']

    if isinstance(p_xle, pd.DataFrame): p_xle = p_xle.iloc[:, 0]
    if isinstance(p_spy, pd.DataFrame): p_spy = p_spy.iloc[:, 0]
    if isinstance(p_oil, pd.DataFrame): p_oil = p_oil.iloc[:, 0]
    if isinstance(p_brent, pd.DataFrame): p_brent = p_brent.iloc[:, 0]

    r_xle = np.log(p_xle / p_xle.shift(1)).dropna()
    r_spy = np.log(p_spy / p_spy.shift(1)).dropna()
    r_oil = np.log(p_oil / p_oil.shift(1)).dropna()
    r_brent = np.log(p_brent / p_brent.shift(1)).dropna()

    common_r = r_xle.index.intersection(r_spy.index).intersection(r_oil.index).intersection(r_brent.index)
    return r_xle.loc[common_r], r_spy.loc[common_r], r_oil.loc[common_r], r_brent.loc[common_r]

def get_common_returns_riskfx(xle, spy, oil, vix, uup):
    xle = xle.between_time("09:30", "16:00")
    spy = spy.between_time("09:30", "16:00")
    oil = oil.between_time("09:30", "16:00")
    vix = vix.between_time("09:30", "16:00")
    uup = uup.between_time("09:30", "16:00")

    common = xle.index.intersection(spy.index).intersection(oil.index).intersection(vix.index).intersection(uup.index)
    p_xle = xle.loc[common]['Close']
    p_spy = spy.loc[common]['Close']
    p_oil = oil.loc[common]['Close']
    p_vix = vix.loc[common]['Close']
    p_uup = uup.loc[common]['Close']

    if isinstance(p_xle, pd.DataFrame): p_xle = p_xle.iloc[:, 0]
    if isinstance(p_spy, pd.DataFrame): p_spy = p_spy.iloc[:, 0]
    if isinstance(p_oil, pd.DataFrame): p_oil = p_oil.iloc[:, 0]
    if isinstance(p_vix, pd.DataFrame): p_vix = p_vix.iloc[:, 0]
    if isinstance(p_uup, pd.DataFrame): p_uup = p_uup.iloc[:, 0]

    r_xle = np.log(p_xle / p_xle.shift(1)).dropna()
    r_spy = np.log(p_spy / p_spy.shift(1)).dropna()
    r_oil = np.log(p_oil / p_oil.shift(1)).dropna()
    r_vix = np.log(p_vix / p_vix.shift(1)).dropna()
    r_uup = np.log(p_uup / p_uup.shift(1)).dropna()

    common_r = r_xle.index.intersection(r_spy.index).intersection(r_oil.index).intersection(r_vix.index).intersection(r_uup.index)
    return r_xle.loc[common_r], r_spy.loc[common_r], r_oil.loc[common_r], r_vix.loc[common_r], r_uup.loc[common_r]

def write_two_factor_summary(label, r_xle, r_spy, r_oil, event_day, out_name):
    is_baseline = r_xle.index < pd.Timestamp(event_day).tz_localize("America/New_York")
    Y = r_xle[is_baseline]
    X = pd.concat([r_spy[is_baseline], r_oil[is_baseline]], axis=1)
    X = sm.add_constant(X)
    model = sm.OLS(Y, X).fit()

    daily_cars = []
    unique_days = np.unique(r_xle[is_baseline].index.date)
    for day in unique_days:
        day_mask = r_xle.index.date == day
        y_d = r_xle[day_mask]
        x_d = pd.concat([r_spy[day_mask], r_oil[day_mask]], axis=1)
        x_d = sm.add_constant(x_d, has_constant="add")
        try:
            exp_d = model.predict(x_d)
            res_d = y_d - exp_d
            daily_cars.append(res_d.sum())
        except Exception:
            pass

    day_mask = r_xle.index.date == pd.Timestamp(event_day).date()
    y_ev = r_xle[day_mask]
    x_ev = pd.concat([r_spy[day_mask], r_oil[day_mask]], axis=1)
    x_ev = sm.add_constant(x_ev, has_constant="add")
    exp_ev = model.predict(x_ev)
    res_ev = y_ev - exp_ev
    car_ev = res_ev.cumsum()

    if daily_cars:
        arr = np.array(daily_cars)
        n_baseline = len(arr)
        final_car = float(car_ev.iloc[-1])
        mean_car = float(arr.mean())
        std_car = float(arr.std())
        exceed = (arr >= final_car).sum()
        p_value = (exceed + 1) / (n_baseline + 1)
        rank = int(exceed + 1)
        summary_lines = [
            f"XLE Intraday CAR Summary ({label})",
            f"Final CAR: {final_car:.6f}",
            f"Baseline Mean CAR: {mean_car:.6f}",
            f"Baseline Std CAR: {std_car:.6f}",
            f"Baseline Days: {n_baseline}",
            f"Empirical p-value (end-of-day CAR): {p_value:.4f}",
            f"Rank (event vs baseline): {rank}/{n_baseline}",
        ]
        summary_path = os.path.join(OUT_DIR, out_name)
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("\n".join(summary_lines) + "\n")
        logger.info(f"Saved {label} CAR summary to {summary_path}")

def build_lagged_factors(r_spy, r_oil, max_lag=1):
    df = pd.DataFrame({
        "SPY": r_spy,
        "WTI": r_oil,
    })
    for lag in range(1, max_lag + 1):
        df[f"SPY_L{lag}"] = df["SPY"].shift(lag)
        df[f"WTI_L{lag}"] = df["WTI"].shift(lag)
    return df.dropna()

def generate_car_chart():
    logger.info("Generating Figure 2: Intraday CAR...")
    
    # 1. Estimate Model (5m Baseline)
    start_base = "2025-11-15"
    end_base = "2026-01-01" 
    
    xle_5m = load_data("XLE", "5m", start_base, "2026-01-05")
    spy_5m = load_data("SPY", "5m", start_base, "2026-01-05")
    oil_5m = load_data("CL=F", "5m", start_base, "2026-01-05")
    brent_5m = load_data("BZ=F", "5m", start_base, "2026-01-05")
    vix_5m = load_data("^VIX", "5m", start_base, "2026-01-05")
    uup_5m = load_data("UUP", "5m", start_base, "2026-01-05")
    uso_5m = load_data("USO", "5m", start_base, "2026-01-05")
    bno_5m = load_data("BNO", "5m", start_base, "2026-01-05")
    
    r_xle, r_spy, r_oil = get_common_returns(xle_5m, spy_5m, oil_5m)
    
    # Baseline Mask
    is_baseline = r_xle.index < pd.Timestamp("2026-01-02").tz_localize("America/New_York")
    
    Y = r_xle[is_baseline]
    X = pd.concat([r_spy[is_baseline], r_oil[is_baseline]], axis=1)
    X = sm.add_constant(X)
    
    model = sm.OLS(Y, X).fit()
    
    if 'SPY' in model.params:
        beta_spy = model.params['SPY']
        beta_oil = model.params['CL=F'] if 'CL=F' in model.params else model.params.iloc[2]
    else:
        beta_spy = model.params[1]
        beta_oil = model.params[2]
        
    logger.info(f"Model Betas (5m): SPY={beta_spy:.2f}, Oil={beta_oil:.2f}")

    # 2. Historical Placebo Paths (Gray Cone)
    daily_paths = []
    unique_days = np.unique(r_xle[is_baseline].index.date)
    
    for day in unique_days:
        day_mask = r_xle.index.date == day
        if not day_mask.any(): continue
        
        y_d = r_xle[day_mask]
        x_d = pd.concat([r_spy[day_mask], r_oil[day_mask]], axis=1)
        x_d = sm.add_constant(x_d, has_constant='add')
        
        try:
            r_exp = model.params['const'] + model.params['SPY']*x_d['SPY'] + model.params[2]*x_d.iloc[:, 2] 
            res_d = y_d - r_exp
            car_d = res_d.cumsum()
            daily_paths.append(car_d.values)
        except Exception:
            pass

    # 3. Calculate Event CAR (1m Resolution)
    xle_1m = load_data("XLE", "1m", "2026-01-02", "2026-01-03")
    spy_1m = load_data("SPY", "1m", "2026-01-02", "2026-01-03")
    oil_1m = load_data("CL=F", "1m", "2026-01-02", "2026-01-03")
    brent_1m = load_data("BZ=F", "1m", "2026-01-02", "2026-01-03")
    vix_1m = load_data("^VIX", "1m", "2026-01-02", "2026-01-03")
    uup_1m = load_data("UUP", "1m", "2026-01-02", "2026-01-03")
    
    # 1m Fallback Check
    if xle_1m is None or xle_1m.empty:
        logger.warning("1m data missing, falling back to 5m")
        xle_ev = xle_5m[xle_5m.index.date == pd.Timestamp("2026-01-02").date()]
        spy_ev = spy_5m[spy_5m.index.date == pd.Timestamp("2026-01-02").date()]
        oil_ev = oil_5m[oil_5m.index.date == pd.Timestamp("2026-01-02").date()]
        brent_ev = brent_5m[brent_5m.index.date == pd.Timestamp("2026-01-02").date()] if brent_5m is not None else None
    else:
        xle_ev, spy_ev, oil_ev = xle_1m, spy_1m, oil_1m
        brent_ev = brent_1m
        vix_ev = vix_1m
        uup_ev = uup_1m

    # Compute Returns & Residuals
    r_xle_ev, r_spy_ev, r_oil_ev = get_common_returns(xle_ev, spy_ev, oil_ev)
    r_exp_ev = model.params['const'] + model.params['SPY'] * r_spy_ev + model.params[2] * r_oil_ev
    res_ev = r_xle_ev - r_exp_ev
    car_ev = res_ev.cumsum()

    # Structural break proxy: CUSUM on intraday residuals
    cusum_stat, break_time = np.nan, None
    if not res_ev.empty:
        cusum_series = (res_ev - res_ev.mean()).cumsum()
        if not cusum_series.empty:
            break_time = cusum_series.abs().idxmax()
            denom = res_ev.std() * np.sqrt(len(res_ev))
            if denom > 0:
                cusum_stat = float(cusum_series.abs().max() / denom)

    # 3C. Three-factor robustness (SPY + WTI + Brent)
    if brent_5m is not None and brent_ev is not None and not brent_ev.empty:
        r_xle_3f, r_spy_3f, r_oil_3f, r_brent_3f = get_common_returns_multi(xle_5m, spy_5m, oil_5m, brent_5m)
        is_baseline_3f = r_xle_3f.index < pd.Timestamp("2026-01-02").tz_localize("America/New_York")
        Y3 = r_xle_3f[is_baseline_3f]
        X3 = pd.concat([r_spy_3f[is_baseline_3f], r_oil_3f[is_baseline_3f], r_brent_3f[is_baseline_3f]], axis=1)
        X3.columns = ["SPY", "WTI", "Brent"]
        X3 = sm.add_constant(X3)
        model3 = sm.OLS(Y3, X3).fit()

        # Baseline daily CARs (3-factor)
        daily_cars_3f = []
        unique_days_3f = np.unique(r_xle_3f[is_baseline_3f].index.date)
        for day in unique_days_3f:
            day_mask = r_xle_3f.index.date == day
            y_d = r_xle_3f[day_mask]
            x_d = pd.concat([r_spy_3f[day_mask], r_oil_3f[day_mask], r_brent_3f[day_mask]], axis=1)
            x_d.columns = ["SPY", "WTI", "Brent"]
            x_d = sm.add_constant(x_d, has_constant="add")
            try:
                exp_d = model3.predict(x_d)
                res_d = y_d - exp_d
                daily_cars_3f.append(res_d.sum())
            except Exception:
                pass

        # Event day CAR (3-factor)
        r_xle_ev_3f, r_spy_ev_3f, r_oil_ev_3f, r_brent_ev_3f = get_common_returns_multi(xle_ev, spy_ev, oil_ev, brent_ev)
        X_ev3 = pd.concat([r_spy_ev_3f, r_oil_ev_3f, r_brent_ev_3f], axis=1)
        X_ev3.columns = ["SPY", "WTI", "Brent"]
        X_ev3 = sm.add_constant(X_ev3, has_constant="add")
        exp_ev3 = model3.predict(X_ev3)
        res_ev3 = r_xle_ev_3f - exp_ev3
        car_ev_3f = res_ev3.cumsum()

        if daily_cars_3f:
            arr = np.array(daily_cars_3f)
            n_baseline = len(arr)
            final_car_3f = float(car_ev_3f.iloc[-1])
            mean_car_3f = float(arr.mean())
            std_car_3f = float(arr.std())
            exceed = (arr >= final_car_3f).sum()
            p_value_3f = (exceed + 1) / (n_baseline + 1)
            rank_3f = int(exceed + 1)
            summary_3f_lines = [
                "XLE Intraday CAR Summary (3-factor: SPY + WTI + Brent)",
                f"Final CAR: {final_car_3f:.6f}",
                f"Baseline Mean CAR: {mean_car_3f:.6f}",
                f"Baseline Std CAR: {std_car_3f:.6f}",
                f"Baseline Days: {n_baseline}",
                f"Empirical p-value (end-of-day CAR): {p_value_3f:.4f}",
                f"Rank (event vs baseline): {rank_3f}/{n_baseline}",
            ]
            summary_3f_path = os.path.join(OUT_DIR, "xle_car_summary_3f.txt")
            with open(summary_3f_path, "w", encoding="utf-8") as f:
                f.write("\n".join(summary_3f_lines) + "\n")
            logger.info(f"Saved 3-factor CAR summary to {summary_3f_path}")

    # 3B. Frequency-consistent robustness (5m baseline + 5m event)
    xle_ev_5m = xle_5m[xle_5m.index.date == pd.Timestamp("2026-01-02").date()]
    spy_ev_5m = spy_5m[spy_5m.index.date == pd.Timestamp("2026-01-02").date()]
    oil_ev_5m = oil_5m[oil_5m.index.date == pd.Timestamp("2026-01-02").date()]
    r_xle_ev_5m, r_spy_ev_5m, r_oil_ev_5m = get_common_returns(xle_ev_5m, spy_ev_5m, oil_ev_5m)
    r_exp_ev_5m = model.params['const'] + model.params['SPY'] * r_spy_ev_5m + model.params[2] * r_oil_ev_5m
    res_ev_5m = r_xle_ev_5m - r_exp_ev_5m
    car_ev_5m = res_ev_5m.cumsum()

    # 3C. Risk/FX robustness (SPY + WTI + VIX + UUP), 5m frequency
    if vix_5m is not None and uup_5m is not None:
        try:
            r_xle_rf, r_spy_rf, r_oil_rf, r_vix_rf, r_uup_rf = get_common_returns_riskfx(
                xle_5m, spy_5m, oil_5m, vix_5m, uup_5m
            )
            is_baseline_rf = r_xle_rf.index < pd.Timestamp("2026-01-02").tz_localize("America/New_York")
            Yrf = r_xle_rf[is_baseline_rf]
            Xrf = pd.concat(
                [r_spy_rf[is_baseline_rf], r_oil_rf[is_baseline_rf], r_vix_rf[is_baseline_rf], r_uup_rf[is_baseline_rf]],
                axis=1
            )
            Xrf.columns = ["SPY", "WTI", "VIX", "UUP"]
            Xrf = sm.add_constant(Xrf)
            model_rf = sm.OLS(Yrf, Xrf).fit()

            daily_cars_rf = []
            unique_days_rf = np.unique(r_xle_rf[is_baseline_rf].index.date)
            for day in unique_days_rf:
                day_mask = r_xle_rf.index.date == day
                y_d = r_xle_rf[day_mask]
                x_d = pd.concat([r_spy_rf[day_mask], r_oil_rf[day_mask], r_vix_rf[day_mask], r_uup_rf[day_mask]], axis=1)
                x_d.columns = ["SPY", "WTI", "VIX", "UUP"]
                x_d = sm.add_constant(x_d, has_constant="add")
                try:
                    exp_d = model_rf.predict(x_d)
                    res_d = y_d - exp_d
                    daily_cars_rf.append(res_d.sum())
                except Exception:
                    pass

            r_xle_ev_rf, r_spy_ev_rf, r_oil_ev_rf, r_vix_ev_rf, r_uup_ev_rf = get_common_returns_riskfx(
                xle_ev_5m, spy_ev_5m, oil_ev_5m, vix_5m, uup_5m
            )
            X_ev_rf = pd.concat([r_spy_ev_rf, r_oil_ev_rf, r_vix_ev_rf, r_uup_ev_rf], axis=1)
            X_ev_rf.columns = ["SPY", "WTI", "VIX", "UUP"]
            X_ev_rf = sm.add_constant(X_ev_rf, has_constant="add")
            exp_ev_rf = model_rf.predict(X_ev_rf)
            res_ev_rf = r_xle_ev_rf - exp_ev_rf
            car_ev_rf = res_ev_rf.cumsum()

            if daily_cars_rf:
                arr = np.array(daily_cars_rf)
                n_baseline_rf = len(arr)
                final_car_rf = float(car_ev_rf.iloc[-1])
                mean_car_rf = float(arr.mean())
                std_car_rf = float(arr.std())
                exceed_rf = (arr >= final_car_rf).sum()
                p_value_rf = (exceed_rf + 1) / (n_baseline_rf + 1)
                rank_rf = int(exceed_rf + 1)
                summary_rf_lines = [
                    "XLE Intraday CAR Summary (5m: SPY + WTI + VIX + UUP)",
                    f"Final CAR: {final_car_rf:.6f}",
                    f"Baseline Mean CAR: {mean_car_rf:.6f}",
                    f"Baseline Std CAR: {std_car_rf:.6f}",
                    f"Baseline Days: {n_baseline_rf}",
                    f"Empirical p-value (end-of-day CAR): {p_value_rf:.4f}",
                    f"Rank (event vs baseline): {rank_rf}/{n_baseline_rf}",
                ]
                summary_rf_path = os.path.join(OUT_DIR, "xle_car_summary_riskfx.txt")
                with open(summary_rf_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(summary_rf_lines) + "\n")
                logger.info(f"Saved risk/FX CAR summary to {summary_rf_path}")
        except Exception:
            logger.exception("Risk/FX robustness failed")

    # 3E. ETF oil controls robustness (USO / BNO), 5m frequency
    try:
        if uso_5m is not None:
            r_xle_uso, r_spy_uso, r_uso = get_common_returns(xle_5m, spy_5m, uso_5m)
            write_two_factor_summary(
                "5m: SPY + USO",
                r_xle_uso,
                r_spy_uso,
                r_uso,
                "2026-01-02",
                "xle_car_summary_uso.txt"
            )
        if bno_5m is not None:
            r_xle_bno, r_spy_bno, r_bno = get_common_returns(xle_5m, spy_5m, bno_5m)
            write_two_factor_summary(
                "5m: SPY + BNO",
                r_xle_bno,
                r_spy_bno,
                r_bno,
                "2026-01-02",
                "xle_car_summary_bno.txt"
            )
    except Exception:
        logger.exception("USO/BNO robustness failed")

    # 3D. Lagged-factor robustness (5m, SPY/WTI lags)
    lag_summary_path = None
    try:
        X_lag = build_lagged_factors(r_spy[is_baseline], r_oil[is_baseline], max_lag=1)
        Y_lag = r_xle[is_baseline].loc[X_lag.index]
        X_lag = sm.add_constant(X_lag)
        model_lag = sm.OLS(Y_lag, X_lag).fit()

        # Baseline daily CARs (lagged model)
        daily_cars_lag = []
        unique_days_lag = np.unique(Y_lag.index.date)
        for day in unique_days_lag:
            day_mask = Y_lag.index.date == day
            y_d = Y_lag[day_mask]
            x_d = X_lag.loc[y_d.index]
            if x_d.empty or y_d.empty:
                continue
            try:
                exp_d = model_lag.predict(x_d)
                res_d = y_d - exp_d
                daily_cars_lag.append(res_d.sum())
            except Exception:
                pass

        # Event day CAR (lagged model, 5m)
        X_ev_lag = build_lagged_factors(r_spy_ev_5m, r_oil_ev_5m, max_lag=1)
        Y_ev_lag = r_xle_ev_5m.loc[X_ev_lag.index]
        X_ev_lag = sm.add_constant(X_ev_lag, has_constant="add")
        exp_ev_lag = model_lag.predict(X_ev_lag)
        res_ev_lag = Y_ev_lag - exp_ev_lag
        car_ev_lag = res_ev_lag.cumsum()

        if daily_cars_lag:
            arr = np.array(daily_cars_lag)
            n_baseline_lag = len(arr)
            final_car_lag = float(car_ev_lag.iloc[-1])
            mean_car_lag = float(arr.mean())
            std_car_lag = float(arr.std())
            exceed_lag = (arr >= final_car_lag).sum()
            p_value_lag = (exceed_lag + 1) / (n_baseline_lag + 1)
            rank_lag = int(exceed_lag + 1)
            summary_lag_lines = [
                "XLE Intraday CAR Summary (5m with SPY/WTI lags)",
                f"Final CAR: {final_car_lag:.6f}",
                f"Baseline Mean CAR: {mean_car_lag:.6f}",
                f"Baseline Std CAR: {std_car_lag:.6f}",
                f"Baseline Days: {n_baseline_lag}",
                f"Empirical p-value (end-of-day CAR): {p_value_lag:.4f}",
                f"Rank (event vs baseline): {rank_lag}/{n_baseline_lag}",
            ]
            lag_summary_path = os.path.join(OUT_DIR, "xle_car_summary_lags.txt")
            with open(lag_summary_path, "w", encoding="utf-8") as f:
                f.write("\n".join(summary_lag_lines) + "\n")
            logger.info(f"Saved lagged-factor CAR summary to {lag_summary_path}")
    except Exception:
        logger.exception("Lagged-factor robustness failed")

    fig, ax = plt.subplots(figsize=(10, 6)) # Size typical for FRL column width approx
    
    # Stats for Cone
    final_cars = [p[-1] for p in daily_paths if len(p) > 10]
    if not final_cars:
        sigma_daily = 0.01 # Fallback
    else:
        sigma_daily = np.std(final_cars)
    
    start_time = car_ev.index[0].replace(hour=9, minute=30)
    end_time = car_ev.index[0].replace(hour=16, minute=0)
    
    # Filter strictly RTH for plot (5m)
    car_ev_5m = car_ev_5m[(car_ev_5m.index >= start_time) & (car_ev_5m.index <= end_time)]
    
    # Create fraction array for cone
    total_sec = (end_time - start_time).total_seconds()
    current_sec = (car_ev_5m.index - start_time).total_seconds()
    t = current_sec / total_sec
    t = np.clip(t, 0, 1) # Bounds
    
    # Square Root Time Scaling for Volatility
    cone_upper = 1.96 * sigma_daily * np.sqrt(t)
    cone_lower = -1.96 * sigma_daily * np.sqrt(t)
    
    ax.fill_between(car_ev_5m.index, cone_upper, cone_lower, color='#999999', alpha=0.3, label='Historical 95% CI')
    
    ax.plot(car_ev_5m.index, car_ev_5m, color='#d62728', linewidth=2, label='Jan 2 Cumulative Abnormal Return (5m)')
    
    ax.set_xlim(start_time, end_time)
    
    locator = mdates.HourLocator(interval=1)
    formatter = mdates.DateFormatter('%H:%M', tz=car_ev_5m.index.tz) # Use Series TZ
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)

    ax.axhline(0, color='black', linewidth=0.8, linestyle='-')

    ax.set_ylabel("Cumulative Abnormal Return (Log)", fontsize=11)
    ax.set_xlabel("Time (EST)", fontsize=11)

    ax.legend(loc='upper left', frameon=False, fontsize=10)
    
    # Stats text
    final_car = car_ev_5m.iloc[-1]
    if final_cars:
        mean_car = float(np.mean(final_cars))
        sigma_daily = float(np.std(final_cars))
        z_score = (final_car - mean_car) / sigma_daily if sigma_daily > 0 else np.nan
    else:
        mean_car = 0.0
        sigma_daily = 0.01
        z_score = final_car / sigma_daily

    if final_cars:
        final_cars_arr = np.array(final_cars)
        n_baseline = len(final_cars_arr)
        exceed = (final_cars_arr >= final_car).sum()
        # Add-one correction to avoid zero p-values in finite samples
        p_value = (exceed + 1) / (n_baseline + 1)
    else:
        n_baseline = 0
        p_value = np.nan

    stats_text = (
        f"Final CAR: {final_car:.2%}\n"
        f"Z-Score: {z_score:.2f} sigma"
    )
    ax.text(
        0.98,
        0.05,
        stats_text,
        transform=ax.transAxes,
        fontsize=10,
        horizontalalignment="right",
        verticalalignment='bottom',
        bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.8),
    )

    rank = None
    if final_cars:
        arr = np.array(final_cars)
        rank = int((arr >= final_car).sum() + 1)
    summary_lines = [
        "XLE Intraday CAR Summary",
        f"Final CAR: {final_car:.6f}",
        f"Baseline Mean CAR: {mean_car:.6f}",
        f"Baseline Std CAR: {sigma_daily:.6f}",
        f"Z-Score: {z_score:.4f}",
        f"Baseline Days: {n_baseline}",
        f"Empirical p-value (end-of-day CAR): {p_value:.4f}",
        f"Rank (event vs baseline): {rank}/{n_baseline}",
        f"CUSUM Statistic: {cusum_stat:.4f}",
        f"CUSUM break time: {break_time}",
    ]
    summary_path = os.path.join(OUT_DIR, "xle_car_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines) + "\n")
    logger.info(f"Saved CAR summary to {summary_path}")

    # save 5m robustness summary
    final_car_5m = car_ev_5m.iloc[-1]
    mean_car_5m = float(np.mean([p[-1] for p in daily_paths if len(p) > 10]))
    std_car_5m = float(np.std([p[-1] for p in daily_paths if len(p) > 10]))
    z_score_5m = (final_car_5m - mean_car_5m) / std_car_5m if std_car_5m > 0 else np.nan
    rank_5m = None
    if final_cars:
        arr_5m = np.array([p[-1] for p in daily_paths if len(p) > 10])
        if arr_5m.size > 0:
            rank_5m = int((arr_5m >= final_car_5m).sum() + 1)
    summary_5m_lines = [
        "XLE Intraday CAR Summary (5m)",
        f"Final CAR: {final_car_5m:.6f}",
        f"Baseline Mean CAR: {mean_car_5m:.6f}",
        f"Baseline Std CAR: {std_car_5m:.6f}",
        f"Z-Score: {z_score_5m:.4f}",
        f"Rank (event vs baseline): {rank_5m}/{len([p for p in daily_paths if len(p) > 10])}",
    ]
    summary_5m_path = os.path.join(OUT_DIR, "xle_car_summary_5m.txt")
    with open(summary_5m_path, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_5m_lines) + "\n")
    logger.info(f"Saved 5m CAR summary to {summary_5m_path}")

    plt.tight_layout()
    out_path = os.path.join(OUT_DIR, "Figure_2_Intraday_CAR.png")
    plt.savefig(out_path)
    logger.info(f"Saved chart to {out_path}")

if __name__ == "__main__":
    generate_car_chart()
