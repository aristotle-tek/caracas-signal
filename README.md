# The Caracas Signal: Replication code

Replication code for the working paper 'The Caracas Signal: Forensic Evidence of Informed Trading Ahead of Operation Stabilize' (Peterson, 2026). Includes intraday spread analysis for XLE, USO, and sector divergence metrics.


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
# Produces Forensic_Chart.png, Hierarchy_Chart.png, and Historical_Control_Chart.png
python src/plots.py
```



## Citation
If you use this code or data, please cite:
> Peterson, Andrew J. (2026). *The Caracas Signal: Forensic Evidence of Informed Trading Ahead of Operation Stabilize*. Working Paper.

## Disclaimer

This report is for academic and informational purposes only. No content herein constitutes financial advice or an accusation of criminal conduct.
