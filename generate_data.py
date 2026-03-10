import pandas as pd
import numpy as np
from datetime import datetime, timedelta
np.random.seed(42)

# 100 sensors, 10 days, 1min intervals = 1.44M readings
sensors = [f"SENSOR_{i:03d}" for i in range(100)]
n_rows = 100 * 1440 * 10  # 1.44M

print(f"Generating {n_rows:,} IoT readings...")

data = []
base_time = datetime(2026, 3, 1)

for sensor in sensors:
    for day in range(10):
        for minute in range(1440):
            timestamp = base_time + timedelta(days=day, minutes=minute)
            
            # Normal: 20-30°C, 40-60% humidity (factory)
            temp = np.random.normal(25, 2)
            humidity = np.random.normal(50, 10)
            
            # Inject anomalies (2% of readings)
            if np.random.random() < 0.02:
                if np.random.random() < 0.5:
                    temp += np.random.normal(15, 2)  # Overheat
                else:
                    humidity = 10 + np.random.random() * 10  # Dry alarm
            
            data.append({
                'timestamp': timestamp,
                'sensor_id': sensor,
                'temperature': max(0, temp),
                'humidity': max(0, humidity),
                'pressure': np.random.normal(1013, 10),  # hPa
                'location': sensor.split('_')[1]
            })

df = pd.DataFrame(data)
df.to_csv('iot_sensors_raw.csv', index=False)
print(f" Saved 1M+ rows to iot_sensors_raw.csv")
print(df.describe())
