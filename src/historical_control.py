import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import os
import glob
import logging
import numpy as np
import plot_style
import statsmodels.api as sm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw_parquet")
OUTPUT_DIR = "out"

UCDP_GED_PATH = os.path.join(DATA_DIR, "ucdp_ged.csv")
EIA_TOP10_PATH = os.path.join(DATA_DIR, "eia_top10_oil.csv")
EVENT_UNIVERSE_PATH = os.path.join(DATA_DIR, "historical_event_universe.csv")

DEFENSE_TICKER = "ITA"
SHIPPING_BASKET = ["FRO", "NAT", "STNG"]
MARKET_TICKER = "SPY"
OIL_TICKER = "CL=F"
FREIGHT_TICKER = "BDRY"

FATALITY_THRESHOLD = 25
QUIET_DAYS = 30
ESTIMATION_WINDOW = 120
ESTIMATION_GAP = 20
MIN_EVENT_YEAR = 2006
CARACAS_DATE = pd.Timestamp("2026-01-02")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def _first_existing_column(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def load_top10_producers(path=EIA_TOP10_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing top-10 producer file: {path}")
    df = pd.read_csv(path)
    year_col = _first_existing_column(df, ["year", "Year"])
    country_col = _first_existing_column(df, ["country", "Country", "name"])
    if year_col is None or country_col is None:
        raise ValueError("Top-10 file must include year and country columns.")
    df[year_col] = df[year_col].astype(int)
    df[country_col] = df[country_col].astype(str).str.strip()
    top10 = {}
    for year, group in df.groupby(year_col):
        top10[year] = set(group[country_col].unique())
    return top10

def build_event_universe(ucdp_path=UCDP_GED_PATH, eia_path=EIA_TOP10_PATH,
                         output_path=EVENT_UNIVERSE_PATH,
                         fatality_threshold=FATALITY_THRESHOLD,
                         quiet_days=QUIET_DAYS):
    if not os.path.exists(ucdp_path):
        raise FileNotFoundError(f"Missing UCDP GED file: {ucdp_path}")
    ucdp = pd.read_csv(ucdp_path, low_memory=False)
    top10 = load_top10_producers(eia_path)

    date_col = _first_existing_column(ucdp, ["date_start", "date", "event_date"])
    country_col = _first_existing_column(ucdp, ["country", "country_name", "country_id"])
    type_col = _first_existing_column(ucdp, ["type_of_violence", "type"])
    fatality_col = _first_existing_column(ucdp, ["best", "deaths", "deaths_best"])
    id_col = _first_existing_column(ucdp, ["id", "event_id", "ged_id"])

    if date_col is None or country_col is None or type_col is None or fatality_col is None:
        raise ValueError("UCDP GED must include date, country, type, and fatality columns.")

    ucdp[date_col] = pd.to_datetime(ucdp[date_col], errors="coerce")
    ucdp = ucdp.dropna(subset=[date_col])
    ucdp["year"] = ucdp[date_col].dt.year.astype(int)
    ucdp[country_col] = ucdp[country_col].astype(str).str.strip()
    ucdp[type_col] = pd.to_numeric(ucdp[type_col], errors="coerce")
    ucdp[fatality_col] = pd.to_numeric(ucdp[fatality_col], errors="coerce").fillna(0.0)

    ucdp = ucdp[ucdp[type_col] == 1]
    ucdp = ucdp[ucdp[fatality_col] >= fatality_threshold]

    def in_top10(row):
        return row["year"] in top10 and row[country_col] in top10[row["year"]]

    ucdp = ucdp[ucdp.apply(in_top10, axis=1)].copy()
    ucdp = ucdp.sort_values([country_col, date_col])

    selected = []
    last_date_by_country = {}
    for _, row in ucdp.iterrows():
        country = row[country_col]
        event_date = row[date_col]
        last_date = last_date_by_country.get(country)
        if last_date is None or (event_date - last_date).days >= quiet_days:
            last_date_by_country[country] = event_date
            selected.append(row)

    if not selected:
        raise ValueError("No events matched the inclusion rule. Check thresholds/data.")

    out = pd.DataFrame(selected)
    out = out.rename(columns={date_col: "event_date", country_col: "country", fatality_col: "fatalities"})
    if id_col is not None and id_col in out.columns:
        out = out.rename(columns={id_col: "event_id"})
    else:
        out["event_id"] = np.arange(len(out))
    out = out[["event_id", "event_date", "country", "fatalities"]].sort_values("event_date")
    out.to_csv(output_path, index=False)
    logger.info(f"Saved event universe to {output_path} ({len(out)} events).")
    return out

def load_event_universe(path=EVENT_UNIVERSE_PATH):
    if os.path.exists(path):
        df = pd.read_csv(path)
        df["event_date"] = pd.to_datetime(df["event_date"])
        return df.sort_values("event_date")
    return build_event_universe()

def download_prices(tickers, start, end):
    data = {}
    for ticker in tickers:
        try:
            df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
            if df.empty:
                logger.warning(f"No data for {ticker}.")
                continue
            data[ticker] = df[["Close"]].copy()
        except Exception as e:
            logger.warning(f"Failed to download {ticker}: {e}")
    return data

def _event_window(trading_days, event_date):
    idx = trading_days.searchsorted(event_date)
    if idx >= len(trading_days):
        return None, None
    t0 = trading_days[idx]
    if idx == 0:
        return None, None
    t_prev = trading_days[idx - 1]
    return t_prev, t0

def _compute_beta(asset_returns, market_returns):
    aligned = pd.concat([asset_returns, market_returns], axis=1).dropna()
    if len(aligned) < 20:
        return None
    x = aligned.iloc[:, 1]
    y = aligned.iloc[:, 0]
    var = np.var(x, ddof=1)
    if var == 0:
        return None
    cov = np.cov(y, x, ddof=1)[0, 1]
    return cov / var

def _load_local_daily_prices(ticker, start, end):
    pattern = os.path.join(RAW_DIR, f"{ticker}_*_1d.parquet")
    files = glob.glob(pattern)
    if not files:
        return None
    frames = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            if isinstance(df.columns, pd.MultiIndex):
                if ("Close", ticker) in df.columns:
                    close = df[("Close", ticker)].rename("Close")
                else:
                    close = df["Close"].iloc[:, 0].rename("Close")
                df = close.to_frame()
            else:
                df = df[["Close"]].copy()
            frames.append(df)
        except Exception as e:
            logger.warning(f"Failed to read {f}: {e}")
    if not frames:
        return None
    merged = pd.concat(frames).sort_index()
    merged = merged[~merged.index.duplicated(keep="last")]
    return merged.loc[(merged.index >= start) & (merged.index <= end)].copy()

def _load_local_intraday_daily_close(ticker, start, end):
    pattern = os.path.join(RAW_DIR, f"{ticker}_*_5m.parquet")
    files = glob.glob(pattern)
    if not files:
        return None
    frames = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            if isinstance(df.columns, pd.MultiIndex):
                if ("Close", ticker) in df.columns:
                    close = df[("Close", ticker)].rename("Close")
                else:
                    close = df["Close"].iloc[:, 0].rename("Close")
                df = close.to_frame()
            else:
                df = df[["Close"]].copy()
            frames.append(df)
        except Exception as e:
            logger.warning(f"Failed to read {f}: {e}")
    if not frames:
        return None
    merged = pd.concat(frames).sort_index()
    merged = merged[~merged.index.duplicated(keep="last")]

    idx = merged.index
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    local_idx = idx.tz_convert("America/New_York")
    merged = merged.copy()
    merged.index = local_idx.tz_localize(None)
    merged = merged.loc[(merged.index >= start) & (merged.index <= end)]
    if merged.empty:
        return None

    daily = merged.groupby(merged.index.date).last()
    daily.index = pd.to_datetime(daily.index)
    return daily.loc[(daily.index >= start) & (daily.index <= end)].copy()

def compute_event_alpha(prices, event_date, ticker, market_ticker=MARKET_TICKER):
    if ticker not in prices or market_ticker not in prices:
        return None, None
    market_df = prices[market_ticker]
    trading_days = market_df.index
    t_prev, t0 = _event_window(trading_days, event_date)
    if t_prev is None:
        return None, None

    asset_df = prices[ticker]
    if t_prev not in asset_df.index or t0 not in asset_df.index:
        return None, None
    if t_prev not in market_df.index or t0 not in market_df.index:
        return None, None

    asset_close_t0 = asset_df.loc[t0, "Close"]
    asset_close_prev = asset_df.loc[t_prev, "Close"]
    market_close_t0 = market_df.loc[t0, "Close"]
    market_close_prev = market_df.loc[t_prev, "Close"]

    if isinstance(asset_close_t0, pd.Series):
        asset_close_t0 = asset_close_t0.iloc[0]
    if isinstance(asset_close_prev, pd.Series):
        asset_close_prev = asset_close_prev.iloc[0]
    if isinstance(market_close_t0, pd.Series):
        market_close_t0 = market_close_t0.iloc[0]
    if isinstance(market_close_prev, pd.Series):
        market_close_prev = market_close_prev.iloc[0]

    asset_event_return = float(asset_close_t0 / asset_close_prev - 1.0)
    market_event_return = float(market_close_t0 / market_close_prev - 1.0)

    asset_returns = asset_df["Close"].pct_change()
    market_returns = market_df["Close"].pct_change()

    t0_idx = trading_days.get_loc(t0)
    start_idx = max(0, t0_idx - ESTIMATION_WINDOW)
    end_idx = max(0, t0_idx - ESTIMATION_GAP)
    if end_idx <= start_idx:
        return None, None

    window_days = trading_days[start_idx:end_idx]
    asset_window = asset_returns.reindex(window_days)
    market_window = market_returns.reindex(window_days)
    beta = _compute_beta(asset_window, market_window)
    if beta is None:
        return None, None

    alpha = asset_event_return - beta * market_event_return
    return alpha, asset_event_return

def _caracas_marker_style(in_subset):
    if in_subset:
        return {"edgecolor": "none", "s": 70}
    return {"edgecolor": "none", "s": 70}

def run_historical_study():
    events = load_event_universe()
    if events.empty:
        logger.warning("No events available.")
        return
    events = events[events["event_date"].dt.year >= MIN_EVENT_YEAR].copy()
    if events.empty:
        logger.warning("No events after MIN_EVENT_YEAR.")
        return

    start = events["event_date"].min() - pd.Timedelta(days=365)
    end = events["event_date"].max() + pd.Timedelta(days=10)
    tickers = [MARKET_TICKER, DEFENSE_TICKER, OIL_TICKER] + SHIPPING_BASKET
    prices = download_prices(tickers, start=start, end=end)

    print(f"\n{'='*95}")
    print(f"{'Event Date':<12} | {'Country':<15} | {'Def Alpha':<10} | {'Ship Alpha':<10} | {'Def Ret':<9}")
    print(f"{'-'*95}")

    rows = []
    for _, row in events.iterrows():
        event_date = row["event_date"]
        country = str(row["country"])

        def_alpha, def_ret = compute_event_alpha(prices, event_date, DEFENSE_TICKER)
        oil_alpha, oil_ret = compute_event_alpha(prices, event_date, OIL_TICKER)

        ship_alphas = []
        ship_rets = []
        for ticker in SHIPPING_BASKET:
            alpha, ret = compute_event_alpha(prices, event_date, ticker)
            if alpha is not None:
                ship_alphas.append(alpha)
            if ret is not None:
                ship_rets.append(ret)

        if def_alpha is None or not ship_alphas:
            event_label = event_date.strftime("%Y-%m-%d") if pd.notna(event_date) else "N/A"
            print(f"{event_label:<12} | {country:<15} | {'N/A':<10} | {'N/A':<10} | {'N/A':<9}")
            continue

        ship_alpha = float(np.mean(ship_alphas))
        def_pct = def_ret * 100 if def_ret is not None else np.nan
        ship_pct = np.mean(ship_rets) * 100 if ship_rets else np.nan

        event_label = event_date.strftime("%Y-%m-%d") if pd.notna(event_date) else "N/A"
        print(f"{event_label:<12} | {country:<15} | {def_alpha:>9.4f} | {ship_alpha:>9.4f} | {def_pct:>8.2f}%")

        rows.append({
            "event_date": event_date,
            "country": country,
            "defense_alpha": def_alpha,
            "shipping_alpha": ship_alpha,
            "defense_return": def_ret,
            "shipping_return": np.mean(ship_rets) if ship_rets else np.nan,
            "oil_alpha": oil_alpha,
            "oil_return": oil_ret
        })

    if not rows:
        logger.warning("No computed events to plot.")
        return

    result_df = pd.DataFrame(rows)
    output_csv = os.path.join(OUTPUT_DIR, "historical_event_alpha.csv")
    result_df.to_csv(output_csv, index=False)
    logger.info(f"Saved event alphas to {output_csv}")
    run_regressions(result_df, os.path.join(OUTPUT_DIR, "historical_regression.txt"))

    run_shipping_freight_check(os.path.join(OUTPUT_DIR, "shipping_freight_check.txt"))

    plot_style.apply_plot_style()
    plt.figure(figsize=(7, 6))
    plt.scatter(
        result_df["defense_alpha"],
        result_df["shipping_alpha"],
        color="#4c78a8",
        alpha=0.8,
        edgecolors="none"
    )

    # Caracas overlay from local daily data (if available)
    local_start = CARACAS_DATE - pd.Timedelta(days=400)
    local_end = CARACAS_DATE + pd.Timedelta(days=10)
    local_prices = {}
    for ticker in [MARKET_TICKER, DEFENSE_TICKER, OIL_TICKER] + SHIPPING_BASKET:
        df = _load_local_daily_prices(ticker, local_start, local_end)
        intraday_df = _load_local_intraday_daily_close(ticker, local_start, local_end)
        frames = []
        if df is not None and not df.empty:
            frames.append(df)
        if intraday_df is not None and not intraday_df.empty:
            frames.append(intraday_df)
        if frames:
            merged = pd.concat(frames).sort_index()
            merged = merged[~merged.index.duplicated(keep="last")]
            local_prices[ticker] = merged
    caracas_alpha = None
    caracas_ship_alpha = None
    caracas_fro_alpha = None
    caracas_oil_alpha = None
    if len(local_prices) >= 2 and DEFENSE_TICKER in local_prices and MARKET_TICKER in local_prices:
        def_alpha, _ = compute_event_alpha(local_prices, CARACAS_DATE, DEFENSE_TICKER)
        oil_alpha_local, _ = compute_event_alpha(local_prices, CARACAS_DATE, OIL_TICKER)
        if oil_alpha_local is not None:
            caracas_oil_alpha = float(oil_alpha_local)
        if "FRO" in local_prices:
            fro_alpha, _ = compute_event_alpha(local_prices, CARACAS_DATE, "FRO")
            if fro_alpha is not None:
                caracas_fro_alpha = fro_alpha
        ship_alphas = []
        for ticker in SHIPPING_BASKET:
            if ticker in local_prices:
                alpha, _ = compute_event_alpha(local_prices, CARACAS_DATE, ticker)
                if alpha is not None:
                    ship_alphas.append(alpha)
        if def_alpha is not None and ship_alphas:
            caracas_alpha = def_alpha
            caracas_ship_alpha = float(np.mean(ship_alphas))

    if caracas_alpha is not None and caracas_ship_alpha is not None:
        logger.info(f"Caracas basket alpha=({caracas_alpha:.4f}, {caracas_ship_alpha:.4f})")
        plt.scatter(
            [caracas_alpha],
            [caracas_ship_alpha],
            color="#1b9e77",
            **_caracas_marker_style(False),
            zorder=3,
            label="Venezuela 2026 (Basket)"
        )
        plt.annotate(
            "Venezuela 2026 (Basket)",
            xy=(caracas_alpha, caracas_ship_alpha),
            xytext=(8, 6),
            textcoords="offset points",
            fontsize=9,
            color="#1b9e77",
            bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.85)
        )
    if caracas_alpha is not None and caracas_fro_alpha is not None:
        logger.info(f"Caracas FRO alpha=({caracas_alpha:.4f}, {caracas_fro_alpha:.4f})")
        plt.scatter(
            [caracas_alpha],
            [caracas_fro_alpha],
            color="#d95f02",
            **_caracas_marker_style(False),
            zorder=3,
            label="Venezuela 2026 (FRO)"
        )
        plt.annotate(
            "Venezuela 2026 (FRO)",
            xy=(caracas_alpha, caracas_fro_alpha),
            xytext=(8, -12),
            textcoords="offset points",
            fontsize=9,
            color="#d95f02",
            bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.85)
        )
    if caracas_alpha is not None and (caracas_ship_alpha is not None or caracas_fro_alpha is not None):
        plt.legend(loc="best", fontsize=9)

    plt.axhline(0, color="gray", linewidth=0.8)
    plt.axvline(0, color="gray", linewidth=0.8)
    plt.xlabel("Defense Alpha (ITA)")
    plt.ylabel("Shipping Alpha (Basket)")
    plt.title("Historical Comparisons: Defense vs Shipping (1-day alpha)")
    plt.tight_layout()

    output_path = os.path.join(OUTPUT_DIR, "Historical_Control_Scatter.png")
    plt.savefig(output_path)
    logger.info(f"Chart saved to {output_path}")

    # Conditional plot: top quartile oil alpha
    q75 = result_df["oil_alpha"].quantile(0.75)
    subset_df = result_df[result_df["oil_alpha"] >= q75].copy()
    if not subset_df.empty:
        plt.figure(figsize=(7, 6))
        plt.scatter(
            subset_df["defense_alpha"],
            subset_df["shipping_alpha"],
            color="#1f77b4",
            alpha=0.85,
            edgecolors="none"
        )
        if caracas_alpha is not None and caracas_ship_alpha is not None:
            in_subset = caracas_oil_alpha is not None and caracas_oil_alpha >= q75
            plt.scatter(
                [caracas_alpha],
                [caracas_ship_alpha],
                color="#d62728",
                **_caracas_marker_style(in_subset),
                zorder=3,
                label="Venezuela 2026 (Basket)"
            )
            plt.annotate(
                "Venezuela 2026 (Basket)",
                xy=(caracas_alpha, caracas_ship_alpha),
                xytext=(8, 6),
                textcoords="offset points",
                fontsize=9,
                color="#d62728",
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.85)
            )
        if caracas_alpha is not None and caracas_fro_alpha is not None:
            in_subset = caracas_oil_alpha is not None and caracas_oil_alpha >= q75
            plt.scatter(
                [caracas_alpha],
                [caracas_fro_alpha],
                color="#2ca02c",
                **_caracas_marker_style(in_subset),
                zorder=3,
                label="Venezuela 2026 (FRO)"
            )
            plt.annotate(
                "Venezuela 2026 (FRO)",
                xy=(caracas_alpha, caracas_fro_alpha),
                xytext=(8, -12),
                textcoords="offset points",
                fontsize=9,
                color="#2ca02c",
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.85)
            )
        if caracas_alpha is not None and (caracas_ship_alpha is not None or caracas_fro_alpha is not None):
            plt.legend(loc="best", fontsize=9)
        plt.axhline(0, color="gray", linewidth=0.8)
        plt.axvline(0, color="gray", linewidth=0.8)
        plt.xlabel("Defense Alpha (ITA)")
        plt.ylabel("Shipping Alpha (Basket)")
        plt.title("Historical Comparisons (Oil Alpha Top Quartile)")
        plt.tight_layout()
        output_conditional = os.path.join(OUTPUT_DIR, "Historical_Control_Scatter_OilQ4.png")
        plt.savefig(output_conditional)
        logger.info(f"Conditional chart saved to {output_conditional}")

    print(f"{'='*95}\n")

def _load_intraday_close_to_close(ticker, event_date):
    pattern_1m = os.path.join(RAW_DIR, f"{ticker}_*_1m.parquet")
    pattern_5m = os.path.join(RAW_DIR, f"{ticker}_*_5m.parquet")
    files = glob.glob(pattern_1m) + glob.glob(pattern_5m)
    if not files:
        return None
    frames = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            if isinstance(df.columns, pd.MultiIndex):
                if ("Close", ticker) in df.columns:
                    close = df[("Close", ticker)].rename("Close")
                else:
                    close = df["Close"].iloc[:, 0].rename("Close")
                df = close.to_frame()
            else:
                df = df[["Close"]].copy()
            frames.append(df)
        except Exception as e:
            logger.warning(f"Failed to read {f}: {e}")
    if not frames:
        return None
    merged = pd.concat(frames).sort_index()
    merged = merged[~merged.index.duplicated(keep="last")]
    idx = merged.index
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    merged.index = idx.tz_convert("America/New_York")
    day_mask = merged.index.date == event_date.date()
    day_df = merged.loc[day_mask]
    if day_df.empty:
        return None
    open_price = day_df["Close"].iloc[0]
    close_price = day_df["Close"].iloc[-1]
    return float(close_price / open_price - 1.0)

def run_shipping_freight_check(output_path):
    event_date = CARACAS_DATE
    results = []
    for ticker in ["FRO", "NAT", "STNG", FREIGHT_TICKER]:
        ret = _load_intraday_close_to_close(ticker, event_date)
        if ret is not None:
            results.append((ticker, ret))

    if not results:
        logger.warning("No intraday freight proxy data available.")
        return

    lines = ["Shipping vs Freight (Jan 2 intraday close/open)"]
    for ticker, ret in results:
        lines.append(f"{ticker}: {ret:.4%}")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    logger.info(f"Freight check saved to {output_path}")

def run_regressions(result_df, output_path):
    cols = ["shipping_alpha", "defense_alpha", "oil_alpha"]
    df = result_df[cols].dropna().copy()
    if len(df) < 10:
        logger.warning("Not enough data for regression output.")
        return

    lines = []

    def fit_and_format(y_col, x_cols, title):
        X = sm.add_constant(df[x_cols])
        y = df[y_col]
        model = sm.OLS(y, X).fit(cov_type="HC1")
        lines.append(title)
        lines.append(f"N={int(model.nobs)}  R2={model.rsquared:.3f}")
        for name, coef, se, pval in zip(model.params.index, model.params, model.bse, model.pvalues):
            lines.append(f"{name:<14} coef={coef:>8.4f}  se={se:>8.4f}  p={pval:>7.4f}")
        lines.append("")

    fit_and_format("shipping_alpha", ["defense_alpha"], "Model 1: shipping_alpha ~ defense_alpha")
    fit_and_format("shipping_alpha", ["defense_alpha", "oil_alpha"], "Model 2: shipping_alpha ~ defense_alpha + oil_alpha")
    fit_and_format("defense_alpha", ["shipping_alpha", "oil_alpha"], "Model 3: defense_alpha ~ shipping_alpha + oil_alpha")

    q75 = df["oil_alpha"].quantile(0.75)
    subset = df[df["oil_alpha"] >= q75]
    lines.append("Conditional summary: oil_alpha >= 75th percentile")
    lines.append(f"N={len(subset)}  oil_alpha_q75={q75:.4f}")
    if not subset.empty:
        for col in ["defense_alpha", "shipping_alpha"]:
            lines.append(
                f"{col:<14} mean={subset[col].mean():>8.4f}  "
                f"p25={subset[col].quantile(0.25):>8.4f}  "
                f"p75={subset[col].quantile(0.75):>8.4f}"
            )
    lines.append("")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")
    logger.info(f"Regression output saved to {output_path}")

if __name__ == "__main__":
    run_historical_study()
