# Project 4: Finance - S&P 500 Anomaly Detection

**1.44M stock trades → Z-Score anomalies**

## Business Impact
🚨 Flash crash detection: TSLA +15.2% (Z=8.1σ)
📈 Volume spikes: AAPL 50K shares (Z=7.9σ)
⚠️ 28K trading alerts (1.96%)
## Pipeline
generate_stock_data.py → 1.44M trades (500 stocks × 10 days)
stock_anomaly_final.py → PySpark Z-Score (|Z|>3σ)
stock_alerts.csv → Trading compliance + HFT signals
