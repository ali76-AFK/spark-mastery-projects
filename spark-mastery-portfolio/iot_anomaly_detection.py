from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark (optimized for your 16GB laptop)
spark = SparkSession.builder \
    .appName("IoT-Anomaly-Detection") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Load 1.44M sensor readings
df = spark.read.option("header", "true").csv("iot_sensors_raw.csv")
df = df.withColumn("timestamp", to_timestamp("timestamp")) \
       .withColumn("temperature", col("temperature").cast("double")) \
       .withColumn("humidity", col("humidity").cast("double")) \
       .withColumn("pressure", col("pressure").cast("double"))

print(" Dataset Overview")
df.select("sensor_id", "temperature", "humidity", "pressure").describe().show()

# Z-Score Anomaly Detection (threshold = 3σ)
print("\n Calculating Z-Scores per sensor...")
temp_stats = df.groupBy("sensor_id").agg(
    mean("temperature").alias("temp_mean"),
    stddev("temperature").alias("temp_std")
)

humid_stats = df.groupBy("sensor_id").agg(
    mean("humidity").alias("humid_mean"),
    stddev("humidity").alias("humid_std")
)

# Broadcast stats (small table → memory efficient)
temp_stats_broadcast = temp_stats.collect()
humid_stats_broadcast = humid_stats.collect()

# Add Z-Scores using broadcast lookup
def calc_temp_zscore(row):
    sensor = row["sensor_id"]
    temp = row["temperature"]
    for stat in temp_stats_broadcast:
        if stat["sensor_id"] == sensor:
            if stat["temp_std"] > 0:
                return abs((temp - stat["temp_mean"]) / stat["temp_std"])
    return 0.0

def calc_humid_zscore(row):
    sensor = row["sensor_id"]
    humid = row["humidity"]
    for stat in humid_stats_broadcast:
        if stat["sensor_id"] == sensor:
            if stat["humid_std"] > 0:
                return abs((humid - stat["humid_mean"]) / stat["humid_std"])
    return 0.0

from pyspark.sql.functions import udf
temp_z_udf = udf(calc_temp_zscore, DoubleType())
humid_z_udf = udf(calc_humid_zscore, DoubleType())

df_with_zscores = df.withColumn("temp_zscore", temp_z_udf(struct("*"))) \
                   .withColumn("humid_zscore", humid_z_udf(struct("*")))

# Flag anomalies (|Z| > 3)
df_anomalies = df_with_zscores.withColumn("is_temp_anomaly", col("temp_zscore") > 3.0) \
                             .withColumn("is_humid_anomaly", col("humid_zscore") > 3.0) \
                             .withColumn("is_anomaly", (col("temp_zscore") > 3.0) | (col("humid_zscore") > 3.0))

# Results Dashboard
print("\n ANOMALY SUMMARY")
anomaly_counts = df_anomalies.select(
    count("*").alias("total_readings"),
    sum("is_anomaly").alias("total_anomalies"),
    sum("is_temp_anomaly").alias("temp_anomalies"),
    sum("is_humid_anomaly").alias("humid_anomalies")
).collect()[0]

print(f"Total readings: {anomaly_counts['total_readings']:,}")
print(f" Temp anomalies: {anomaly_counts['temp_anomalies']:,} ({anomaly_counts['temp_anomalies']/anomaly_counts['total_readings']*100:.1f}%)")
print(f" Humid anomalies: {anomaly_counts['humid_anomalies']:,} ({anomaly_counts['humid_anomalies']/anomaly_counts['total_readings']*100:.1f}%)")
print(f"  Total anomalies: {anomaly_counts['total_anomalies']:,} ({anomaly_counts['total_anomalies']/anomaly_counts['total_readings']*100:.1f}%)")

# TOP 5 Worst Sensors
print("\n TOP 5 PROBLEM SENSORS")
df_anomalies.filter(col("is_anomaly") == True) \
    .groupBy("sensor_id").agg(count("*").alias("anomaly_count")) \
    .orderBy(desc("anomaly_count")).limit(5).show()

# Save alerts
df_anomalies.filter(col("is_anomaly") == True) \
    .select("timestamp", "sensor_id", "temperature", "humidity", "temp_zscore", "humid_zscore", "location") \
    .coalesce(1).write.mode("overwrite").option("header", "true").csv("iot_alerts")

# Performance
print("\n  Spark Job Complete!")
print(f"Partitions: {df.rdd.getNumPartitions()}")
spark.stop()