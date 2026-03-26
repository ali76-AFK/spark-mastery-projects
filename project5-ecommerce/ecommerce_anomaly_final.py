#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Ecommerce-Anomaly").getOrCreate()

# Load 1.44M e-commerce orders
df = spark.read.option("header", "true").csv("ecommerce_orders_raw.csv")
df = df.withColumn("quantity", col("quantity").cast("int")) \
       .withColumn("unit_price", col("unit_price").cast("double")) \
       .withColumn("discount_pct", col("discount_pct").cast("double")) \
       .withColumn("total_amount", col("total_amount").cast("double"))

print("🛒 Dataset: {:,} orders".format(df.count()))
df.select("category", "quantity", "unit_price", "discount_pct", "total_amount").describe().show()

# E-commerce anomaly detection metrics
stats = df.agg(
    mean("quantity").alias("qty_mean"),
    stddev("quantity").alias("qty_std"),
    mean("discount_pct").alias("disc_mean"),
    stddev("discount_pct").alias("disc_std"),
    mean("total_amount").alias("amt_mean"),
    stddev("total_amount").alias("amt_std")
).collect()[0]

print("💰 Stats:")
print("Quantity: μ={:.1f} ± {:.1f}σ".format(stats["qty_mean"], stats["qty_std"]))
print("Discount: μ={:.1f}% ± {:.1f}σ".format(stats["disc_mean"], stats["disc_std"]))
print("Amount:  μ=${:.0f} ± ${:.0f}σ".format(stats["amt_mean"], stats["amt_std"]))

# Fraud patterns (|Z| > 3σ)
df_anomalies = df.withColumn("qty_zscore", 
    abs((col("quantity") - stats["qty_mean"]) / stats["qty_std"])
).withColumn("disc_zscore", 
    abs((col("discount_pct") - stats["disc_mean"]) / stats["disc_std"])
).withColumn("amt_zscore", 
    abs((col("total_amount") - stats["amt_mean"]) / stats["amt_std"])
).filter((col("qty_zscore") > 3) | (col("disc_zscore") > 3) | (col("amt_zscore") > 3))

print("🚨 E-COMMERCE FRAUD: {:,} alerts ({:.1f}%)".format(
    df_anomalies.count(), df_anomalies.count() / df.count() * 100))

print("🔥 Top 20 fraud alerts:")
df_anomalies.select("timestamp", "customer_id", "product", "quantity", 
                   "discount_pct", "total_amount", "qty_zscore", "disc_zscore", "amt_zscore") \
           .orderBy(col("qty_zscore").desc()).limit(20).show(truncate=False)

# Save fraud alerts
df_anomalies.coalesce(1).write.mode("overwrite").option("header", "true").csv("ecommerce_alerts")
print("💾 Fraud alerts saved → ecommerce_alerts/part-*.csv")

# Category revenue ranking
revenue_by_category = df.groupBy("category").agg(
    sum("total_amount").alias("total_revenue"),
    count("*").alias("order_count"),
    avg("total_amount").alias("avg_order_value")
).orderBy(col("total_revenue").desc())

print("\n🏆 Revenue by Category:")
revenue_by_category.show()

spark.stop()