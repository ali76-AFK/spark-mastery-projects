from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("IoT-Anomaly-Detection-v2") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

df = spark.read.option("header", "true").csv("iot_sensors_raw.csv")
df = df.withColumn("timestamp", to_timestamp("timestamp")) \
       .withColumn("temperature", col("temperature").cast("double")) \
       .withColumn("humidity", col("humidity").cast("double")) \
       .withColumn("pressure", col("pressure").cast("double"))

print(" Dataset loaded:", df.count(), "rows")
df.select("sensor_id", "temperature", "humidity", "pressure").describe().show()

# Global Z-Score stats
temp_stats = df.agg(mean("temperature").alias("temp_mean"), stddev("temperature").alias("temp_std")).collect()[0]
humid_stats = df.agg(mean("humidity").alias("humid_mean"), stddev("humidity").alias("humid_std")).collect()[0]

print(f"  Temp: μ={temp_stats['temp_mean']:.1f}σ σ={temp_stats['temp_std']:.1f}")
print(f" Humid: μ={humid_stats['humid_mean']:.1f}σ σ={humid_stats['humid_std']:.1f}")

def temp_zscore(temp):
    if temp_stats['temp_std'] > 0:
        return abs((float(temp) - float(temp_stats['temp_mean'])) / float(temp_stats['temp_std']))
    return 0.0

def humid_zscore(humid):
    if humid_stats['humid_std'] > 0:
        return abs((float(humid) - float(humid_stats['humid_mean'])) / float(humid_stats['humid_std']))
    return 0.0

from pyspark.sql.functions import udf
temp_z_udf = udf(temp_zscore, DoubleType())
humid_z_udf = udf(humid_zscore, DoubleType())

df_zscores = df.withColumn("temp_zscore", temp_z_udf("temperature")) \
               .withColumn("humid_zscore", humid_z_udf("humidity"))

df_anomalies = df_zscores.withColumn("is_temp_anomaly", col("temp_zscore") > 3.0) \
                        .withColumn("is_humid_anomaly", col("humid_zscore") > 3.0) \
                        .withColumn("is_anomaly", (col("temp_zscore") > 3.0) | (col("humid_zscore") > 3.0))

# FIXED: Cast boolean to int
anomaly_counts = df_anomalies.select(
    count("*").alias("total_readings"),
    sum(col("is_temp_anomaly").cast("int")).alias("temp_anomalies"),
    sum(col("is_humid_anomaly").cast("int")).alias("humid_anomalies"),
    sum(col("is_anomaly").cast("int")).alias("total_anomalies")
).collect()[0]

print("\n ANOMALY SUMMARY")
print(f" Total: {anomaly_counts['total_readings']:,}")
print(f" Temp anomalies: {anomaly_counts['temp_anomalies']:,} ({anomaly_counts['temp_anomalies']/anomaly_counts['total_readings']*100:.1f}%)")
print(f" Humidity anomalies: {anomaly_counts['humid_anomalies']:,} ({anomaly_counts['humid_anomalies']/anomaly_counts['total_readings']*100:.1f}%)")
print(f"  Total alerts: {anomaly_counts['total_anomalies']:,} ({anomaly_counts['total_anomalies']/anomaly_counts['total_readings']*100:.1f}%)")

print("\n TOP 5 PROBLEM SENSORS")
df_anomalies.filter(col("is_anomaly") == True) \
    .groupBy("sensor_id").agg(count("*").alias("alerts")) \
    .orderBy(col("alerts").desc()).limit(5).show()

df_anomalies.filter(col("is_anomaly") == True) \
    .select("timestamp", "sensor_id", "temperature", "humidity", "temp_zscore", "humid_zscore") \
    .coalesce(1).write.mode("overwrite").option("header", "true").csv("iot_alerts_final")

print("\n Alerts saved to iot_alerts_final/")
spark.stop()
print(" COMPLETE!")
