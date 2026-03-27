#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Stock-Anomaly").getOrCreate()

# Load S&P 500 trades
df = spark.read.option("header", "true").csv("stock_trades_raw.csv")
df = df.withColumn("price", col("price").cast("double")) \
       .withColumn("volume", col("volume").cast("long")) \
       .withColumn("price_change_pct", col("price_change_pct").cast("double"))

print("📈 Dataset: {:,} trades".format(df.count()))
df.select("ticker", "price", "volume", "price_change_pct").describe().show()

# Z-Score anomaly detection (financial)
stats = df.agg(
    mean("price_change_pct").alias("price_mean"),
    stddev("price_change_pct").alias("price_std"),
    mean("volume").alias("volume_mean"),
    stddev("volume").alias("volume_std")
).collect()[0]

print("💹 Stats: Price μ={:.2f}% ± {:.2f}σ | Volume μ={:.0f} ± {:.0f}σ".format(
    stats["price_mean"], stats["price_std"], stats["volume_mean"], stats["volume_std"]))

# Financial anomalies (|Z| > 3σ)
df_anomalies = df.withColumn("price_zscore", 
    abs((col("price_change_pct") - stats["price_mean"]) / stats["price_std"])
).withColumn("volume_zscore", 
    abs((col("volume") - stats["volume_mean"]) / stats["volume_std"])
).filter((col("price_zscore") > 3) | (col("volume_zscore") > 3)) \
 .select("timestamp", "ticker", "price", "volume", "price_change_pct", "price_zscore", "volume_zscore")

print("🚨 STOCK ANOMALIES: {:,}".format(df_anomalies.count()))
df_anomalies.orderBy(col("price_zscore").desc()).show(20, truncate=False)

# Save alerts
df_anomalies.coalesce(1).write.mode("overwrite").option("header", "true").csv("stock_alerts")
print("💾 Saved: stock_alerts/part-*.csv")
spark.stop()
