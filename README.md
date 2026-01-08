# The Caracas Signal: Replication code

Replication code for the working paper 'The Caracas Signal: Forensic Evidence of Informed Trading Ahead of Operation Stabilize' (Peterson, 2026). Includes intraday spread analysis for XLE, USO, and sector divergence metrics.


## Key Findings

1.  The Leak: Energy equities (XLE) decoupled from crude oil futures (CL=F) starting at 14:55 EST on Jan 2, roughly 13 hours before the operation commenced.
2.  The Scope (Signal): While Defense stocks (ITA) rallied (+3.41%), Shipping stocks (FRO) crashed (-5.68%). 
    * *Interpretation:* This divergence indicates the "smart money" priced in a surgical, stabilizing strike (which lowers shipping risk) rather than a protracted conflict (which typically raises shipping risk premiums).
3.  Magnitude: The spread between equities and commodities reached +1.94%, .a statistical outlier measuring 2.69 standard deviations above the mean of baseline intraday volatility.


The chart below visualizes the "decoupling" moment where energy stocks broke away from their historical correlation with oil futures.

![Sector-Commodity Decoupling](out/fig1.png)


## Repository Structure

```text
/caracas-signal/
│
├── data/                    # store parquet files here
├── out/                     # generated output
├── src/
│   ├── data_loader.py       # Data acquisition & caching
│   ├── forensics.py         # Main anomaly detection script
│   ├── plots.py             # Chart generation
│   ├── historical_control.py# Event study comparison script
│   ├── factor_model.py      # Intraday regression model
│   ├── polymarket_control.py# Prediction market analysis
│   └── models.py            # Daily beta calculations
│
└── requirements.txt
```


## Replication

To reproduce the findings and generate the charts:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. download data (~30 days of 5m data)
python src/data_loader.py

# 3. Run analysis (Decoupling & shipping basket)
python src/forensics.py

# 4. Run robustness checks (Factor model, historical, Polymarket)
python src/factor_model.py
python src/historical_control.py
python src/polymarket_control.py

# 5. Plots
python src/plots.py
```



## Citation and License
If you use this code or data, please cite:
> Peterson, Andrew J. (2026). *The Caracas Signal: Forensic Evidence of Informed Trading Ahead of Operation Stabilize*. Working Paper.

This repository is dual-licensed:

1. Code: The source code in this repository is licensed under the MIT License. You are free to use, modify, and distribute the code as long as the copyright notice is preserved.
2. Research Assets: The figures, datasets, and the paper ("The Caracas Signal") are licensed under CC BY 4.0. You are free to share and adapt these materials, but you must give appropriate credit to the author (Andrew J. Peterson) and provide a link to the original work.

See `LICENSE` for the full MIT text and [creativecommons.org](https://creativecommons.org/licenses/by/4.0/) for the CC BY 4.0 terms.



## Disclaimer

This report is for academic and informational purposes only. No content herein constitutes financial advice or an accusation of criminal conduct.
