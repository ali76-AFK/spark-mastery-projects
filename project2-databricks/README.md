# Project 2: IoT Anomaly Detection → Databricks Cloud

**Same PySpark code → 28GB cluster → 3-4x speedup**

## Performance Comparison
| Environment | Runtime | Alerts | Cluster |
|-------------|---------|--------|---------|
| **Local Laptop** | 2:30 min | 28,322 | 16GB |
| **Databricks** | **~45 sec** | **28,321** | **28GB** |

## Key Databricks Learnings
✅ %%sh cell magic (multi-line shell)
✅ /Workspace/Users/... absolute paths
✅ Serverless → Unity Catalog tables (not local files)
✅ Spark UI + cluster monitoring

