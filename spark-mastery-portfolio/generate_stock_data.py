#!/usr/bin/env python3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# S&P 500 tickers (top 20 for demo)
tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'JPM', 'UNH', 'V',
           'JNJ', 'PG', 'HD', 'MA', 'CVX', 'ABBV', 'BAC', 'KO', 'WMT', 'DIS']

np.random.seed(42)
start_date = datetime(2026, 3, 16)  # Trading days
n_trades_per_minute = 1440  # 500 stocks × avg 2.88 trades/min
total_trades = 1440000

print("Generating {} S&P 500 trades...".format(total_trades))

data = []
trade_id = 0
for minute in range(total_trades // n_trades_per_minute):
    date = start_date + timedelta(days=minute // (6*60), minutes=minute % (6*60))
    if date.weekday() >= 5: continue  # Skip weekends
    
    for ticker in random.choices(tickers, k=n_trades_per_minute):
        trade_id += 1
        
        # Base price + volatility
        base_price = random.uniform(50, 500)
        price_change = np.random.normal(0, base_price * 0.01)  # 1% volatility
        volume = random.choices([100, 500, 1000, 5000, 10000], weights=[0.4, 0.3, 0.2, 0.08, 0.02])[0]
        
        # Anomalies (2%)
        if random.random() < 0.02:
            price_change *= random.choice([5, -5])  # Flash crash/pump
            volume *= random.choice([3, 10])        # Volume spike
        
        price = base_price + price_change
        
        data.append({
            'trade_id': trade_id,
            'timestamp': date.strftime('%Y-%m-%d %H:%M:%S'),
            'ticker': ticker,
            'price': round(max(price, 0.01), 2),
            'volume': volume,
            'price_change_pct': round(price_change / base_price * 100, 2)
        })

df = pd.DataFrame(data[:total_trades])  # Exact 1.44M
df.to_csv('stock_trades_raw.csv', index=False)
print("✅ Saved: stock_trades_raw.csv ({:,} trades, {:.0f}MB)".format(
    len(df), df.memory_usage(deep=True).sum() / 1e6))
print("📊 Stats:")
print(df[['price', 'volume', 'price_change_pct']].describe())
