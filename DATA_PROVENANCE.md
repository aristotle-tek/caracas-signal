# Data Provenance

## Intraday price data
- Source: Yahoo Finance via `yfinance`
- Adjustment: `auto_adjust=True` in downloads
- Time zone: normalized to `America/New_York` in the analysis pipeline

## Cached data
Intraday data are cached in `data/raw_parquet/` to ensure reproducibility given vendor retention limits. However, these files are not bundled in the replication package due to licensing restrictions.
The replication package ships with an empty `data/raw_parquet/` folder.

To recreate cached intraday data, use `src/data_loader.py` with the event date and baseline window. Yahoo Finance intraday data typically expire after ~7 days for 1-minute bars and ~60 days for 5-minute bars, and may not be recoverable once the window closes.

## External datasets
- UCDP GED: `data/ucdp_ged.csv` (Obtain from [UCDP website](https://ucdp.uu.se/downloads/) : UCDP Georeferenced Event Dataset (GED) Global version 25.1)
- EIA oil production: `data/eia_top10_oil.csv`
- Precomputed event universe (for reproducibility): `data/historical_event_universe.csv`
- Polymarket price history: `data/polymarket/polymarket-price-data-07-09-2025-07-01-2026-1767785813564.csv`

## Licensing note
Yahoo Finance data are used for academic replication purposes. Redistribution of cached intraday data are restricted.
