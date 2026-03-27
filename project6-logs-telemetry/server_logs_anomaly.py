#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Server-Logs-Anomaly").getOrCreate()

# Load 1.44M server logs
df = spark.read.option("header", "true").csv("server_logs_raw.csv")
df = df.withColumn("response_time_ms", col("response_time_ms").cast("double")) \
       .withColumn("bytes_sent", col("bytes_sent").cast("double")) \
       .withColumn("status_code", col("status_code").cast("int"))

print("🖥️  Dataset: {:,} server logs".format(df.count()))
df.select("server_id", "endpoint", "status_code", "response_time_ms").describe().show()

# Calculate anomaly thresholds
stats = df.agg(
    mean("response_time_ms").alias("rt_mean"),
    stddev("response_time_ms").alias("rt_std"),
    mean("bytes_sent").alias("bytes_mean"),
    stddev("bytes_sent").alias("bytes_std"),
    sum((col("status_code") >= 400).cast("int")).alias("error_count")
).collect()[0]

print("⚡ Performance Stats:")
print(f"Response Time: μ={stats['rt_mean']:.0f}±{stats['rt_std']:.0f}ms")
print(f"Error Rate: {stats['error_count']/df.count()*100:.1f}%")

# Detect anomalies (Z > 3σ)
df_anomalies = df.withColumn("rt_zscore", 
    abs((col("response_time_ms") - stats["rt_mean"]) / stats["rt_std"])
).withColumn("bytes_zscore", 
    abs((col("bytes_sent") - stats["bytes_mean"]) / stats["bytes_std"])
).filter((col("rt_zscore") > 3) | (col("status_code") >= 400) | (col("bytes_zscore") > 3))

print("🚨 SYSTEM ALERTS: {:,} anomalies ({:.1f}%)".format(
    df_anomalies.count(), df_anomalies.count()/df.count()*100))

print("🔥 Top 10 critical incidents:")
df_anomalies.filter((col("status_code") >= 400) | (col("rt_zscore") > 5)) \
    .select("timestamp", "server_id", "endpoint", "status_code", "response_time_ms", "rt_zscore") \
    .orderBy(col("rt_zscore").desc()).limit(10).show(truncate=False)

# Save alerts
df_anomalies.coalesce(1).write.mode("overwrite").option("header", "true").csv("server_alerts")
print("💾 Alerts saved → server_alerts/part-*.csv")

spark.stop()
