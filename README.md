# 🚀 Spark Mastery: Production Projects

**AI/Robotics Portfolio** - PySpark across 5 domains + 4 environments

## Project ✅ IoT Anomaly Detection (Local Spark 3.5.1)

**1.44M factory sensors → 28,322 anomalies** (1.96%) in **2:30 min** (16GB laptop)
### Live Results

 Max temp: 44.3°C (Z=7.66σ OVERHEAT)
 Max humid: 81.4% (Z=3.01σ)
 Total alerts: 28,322 from 1,440,000 readings
 
 ### Run It
```bash
python3 generate_data.py      # 1.44M sensors (125MB)
spark-submit iot_anomaly_final.py  # 28K alerts (3.3MB)
``` 
 Architecture

text
CSV → PySpark DataFrames → Z-Score (|Z|>3σ) → Alerts CSV
temp_z = abs((temp - 25.1°C) / 2.5σ)
