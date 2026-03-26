# Project 3 ✓ Docker Spark Cluster (Production)

**IoT Pipeline Deployed Across 3 Environments**

## Performance Matrix
| Environment | Nodes | Est. Runtime | Alerts | Tech Stack |
|-------------|-------|--------------|--------|------------|
| **Local** | 1 | **2:30min** | **28,322** | PySpark |
| **Databricks** | Serverless | **~45sec** | **28,321** | Cloud |
| **Docker** | **1 Master + 1 Worker** | **~1:30min** | **28K** | **Containers** |

## Docker Cluster Specs
git clone https://github.com/big-data-europe/docker-spark
docker-compose up -d
spark-master:8080 → spark://897f2d25d4e0:7077
18 cores | 14GB RAM | bde2020/spark:3.3.0-hadoop3.3
## Production Skills Demonstrated
- Docker Compose multi-container orchestration
- Spark Standalone cluster deployment  
- Master/Worker architecture
- Port mapping (8080, 7077, 8081)
- Health monitoring via Spark UI
