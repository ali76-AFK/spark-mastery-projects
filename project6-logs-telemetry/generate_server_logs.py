#!/usr/bin/env python3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

np.random.seed(42)
print("🖥️  Generating 1.44M Apache/Nginx server logs...")

# Server fleet + realistic endpoints
servers = [f"web{i:02d}" for i in range(1,11)]
endpoints = [
    "/api/v1/users", "/api/v1/orders", "/dashboard", "/login", "/checkout",
    "/api/v2/products", "/search", "/cart", "/profile", "/admin"
]
user_agents = ["Mozilla/5.0 (Chrome)", "Mozilla/5.0 (Safari)", "curl/7.68", "PostmanRuntime"]

# Generate 1.44M logs (10 servers × 10 days × ~14.4k req/hour)
n_logs = 1_440_000
logs = []

base_time = datetime(2026, 3, 17)
for i in range(n_logs):
    timestamp = base_time + timedelta(seconds=np.random.exponential(0.25))
    server = random.choice(servers)
    endpoint = random.choice(endpoints)
    
    # Simulate anomalies (1-2%)
    if np.random.random() < 0.01:  # DDoS spikes
        response_time = np.random.exponential(5) * 1000  # 5s+ latency
        status_code = random.choice([404, 500, 503])
    elif np.random.random() < 0.005:  # Server outage
        response_time = np.random.exponential(10) * 1000  # 10s+ latency
        status_code = 503
    else:  # Normal traffic
        response_time = np.random.exponential(0.2) * 1000  # 200ms avg
        status_code = 200 if np.random.random() < 0.95 else 404
    
    logs.append({
        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'server_id': server,
        'endpoint': endpoint,
        'method': random.choice(['GET', 'POST']),
        'status_code': status_code,
        'response_time_ms': max(0, round(response_time, 2)),
        'bytes_sent': np.random.normal(5000, 2000),
        'user_agent': random.choice(user_agents),
        'ip_address': f"192.168.{random.randint(1,255)}.{random.randint(1,255)}"
    })

df = pd.DataFrame(logs)
df.to_csv('server_logs_raw.csv', index=False)
print(f"💾 Saved: server_logs_raw.csv ({df.memory_usage(deep=True).sum()//1024//1024}MB)")
print(f"📊 Summary: {len(df):,} logs | Avg: {df['response_time_ms'].mean():.0f}ms | Errors: {len(df[df['status_code']>=400]):,} ({len(df[df['status_code']>=400])/len(df)*100:.1f}%)")
