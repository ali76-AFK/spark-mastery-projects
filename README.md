# Spark Mastery

PySpark-based anomaly detection portfolio across IoT, finance, e-commerce, logs, and social media.

## Overview

Spark Mastery is a portfolio of production-style PySpark pipelines built to process large synthetic and semi-realistic datasets across multiple domains.  
The project demonstrates distributed data processing, anomaly detection, Dockerized execution, and repeatable pipeline design.

Across the portfolio, the pipelines identify unusual patterns in sensor readings, trading activity, purchase behavior, server logs, and social media engagement.

## Portfolio Highlights

- 7.2M+ records processed across multiple domains.
- 299K anomalies detected using Z-score-based detection.
- Dockerized Spark 3.5.1 execution environment.
- PySpark DataFrames and SQL-based pipeline design.
- End-to-end examples for data generation, processing, and alert export.
- Demonstrates a strong mix of data engineering, analytics, and operational reasoning.

## Architecture

Each pipeline follows a similar pattern:

```text
Raw CSV data → PySpark DataFrames → Feature engineering / Z-score detection → Alert export CSV
```

The pipelines are designed to be reproducible locally and in Docker, with clear input and output directories.

## Project Structure

```text
spark-mastery-projects/
├── data/                  # Input datasets
├── pipelines/             # Main PySpark anomaly detection scripts
├── generate_data/         # Synthetic data generators
├── outputs/               # Generated anomaly alert files
├── Dockerfile.spark-pipeline
└── README.md
```

## Individual Projects

### 1. IoT Sensor Anomaly Detection
- 1.44M sensor readings processed.
- Detects temperature, vibration, and pressure anomalies.
- Output: 28K factory alerts.

### 2. Financial Market Anomaly Detection
- 1.44M stock trades processed.
- Detects price and volume anomalies.
- Output: 35K trading alerts.

### 3. E-commerce Fraud Detection
- 1.44M orders processed.
- Detects bulk orders, coupon abuse, and suspicious purchasing patterns.
- Output: 28K fraud alerts.

### 4. Server Logs / SRE Anomaly Detection
- 1.44M Apache/Nginx log rows processed.
- Detects outages, latency spikes, and error bursts.
- Output: 92K operational alerts.

### 5. Social Media Trends + Bot Detection
- 1.44M social media posts processed.
- Detects viral trend spikes and bot-like behavior.
- Output: 116K trend and bot alerts.

## Example Results

| Domain | Records | Alerts | Use Case |
|---|---:|---:|---|
| IoT Sensors | 1.44M | 28K | Predictive maintenance |
| Financial HFT | 1.44M | 35K | Flash crash and trading anomaly detection |
| E-commerce | 1.44M | 28K | Fraud and abuse detection |
| Server Logs | 1.44M | 92K | SRE and outage detection |
| Social Media | 1.44M | 116K | Trend and bot detection |

## Production Docker Run

### Build
```bash
docker build -f Dockerfile.spark-pipeline -t spark-mastery:v1 .
```

### Run a pipeline
```bash
docker run --rm -v $(pwd)/data:/app/data spark-mastery:v1 \
  /opt/spark/bin/spark-submit --master local /app/ecommerce_anomaly_final.py
```

## Production Verification

- Spark 3.5.1 container execution.
- MemoryStore and Spark UI verified.
- Data volumes mounted successfully.
- Spark submit path resolved inside container.
- Pipelines run without external network dependency.

## Technical Challenges Solved

- Fixed Docker image compatibility.
- Resolved Spark submit path issues.
- Standardized volume mounting.
- Fixed dataset path handling in PySpark scripts.
- Built network-independent Docker execution.

## Future Improvements

- Add Spark Streaming for real-time anomaly detection.
- Add MLflow for experiment tracking.
- Deploy through Kubernetes Spark Operator.
- Add Grafana dashboards for visualization.
- Validate in a cloud-based Databricks environment.

## Notes

This repository is intended for portfolio and learning purposes.  
The pipelines are designed to show distributed processing, anomaly detection, and reproducible engineering workflows.
