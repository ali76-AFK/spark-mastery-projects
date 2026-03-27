from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("IoT-Anomaly-Final") \
    .config("spark.driver.memory", "6g") \
    .getOrCreate()

# Load + clean data
df = spark.read.option("header", "true").csv("iot_sensors_raw.csv")
df = df.withColumn("timestamp", to_timestamp("timestamp")) \
       .withColumn("temperature", col("temperature").cast("double")) \
       .withColumn("humidity", col("humidity").cast("double")) \
       .withColumn("pressure", col("pressure").cast("double"))

print(" Dataset:", df.count(), "rows")
df.select("sensor_id", "temperature", "humidity").describe().show()

# Global Z-Score stats (pure Spark)
stats = df.agg(
    mean("temperature").alias("temp_mean"),
    stddev("temperature").alias("temp_std"),
    mean("humidity").alias("humid_mean"), 
    stddev("humidity").alias("humid_std")
).collect()[0]

print(f"  Temp: μ={stats['temp_mean']:.1f}±{stats['temp_std']:.1f}")
print(f" Humid: μ={stats['humid_mean']:.1f}±{stats['humid_std']:.1f}")

# Z-Scores with PURE DataFrame ops (NO UDFs!)
df_zscores = df.withColumn("temp_zscore", 
    abs((col("temperature") - stats['temp_mean']) / stats['temp_std'])
).withColumn("humid_zscore", 
    abs((col("humidity") - stats['humid_mean']) / stats['humid_std'])
)

# Flag anomalies
df_anomalies = df_zscores.withColumn("is_temp_anomaly", col("temp_zscore") > 3) \
                        .withColumn("is_humid_anomaly", col("humid_zscore") > 3) \
                        .withColumn("is_anomaly", (col("temp_zscore") > 3) | (col("humid_zscore") > 3))

# FIXED aggregations
summary = df_anomalies.select(
    count("*").alias("total"),
    sum(col("is_temp_anomaly").cast("int")).alias("temp_alerts"),
    sum(col("is_humid_anomaly").cast("int")).alias("humid_alerts"),
    sum(col("is_anomaly").cast("int")).alias("total_alerts")
).collect()[0]

print("\n ANOMALY DASHBOARD")
print(f" Total readings: {summary['total']:,}")
print(f" Temp anomalies: {summary['temp_alerts']:,} ({summary['temp_alerts']/summary['total']*100:.1f}%)")
print(f" Humid anomalies: {summary['humid_alerts']:,} ({summary['humid_alerts']/summary['total']*100:.1f}%)")
print(f"  TOTAL ALERTS: {summary['total_alerts']:,} ({summary['total_alerts']/summary['total']*100:.1f}%)")

# Worst sensors
print("\n TOP 5 PROBLEM SENSORS")
df_anomalies.filter(col("is_anomaly")) \
    .groupBy("sensor_id").agg(count("*").alias("alerts")) \
    .orderBy(col("alerts").desc()).limit(5).show()

# Save alerts
df_anomalies.filter(col("is_anomaly")).select(
    "timestamp", "sensor_id", "temperature", "humidity", 
    "temp_zscore", "humid_zscore", "location"
).coalesce(1).write.mode("overwrite").option("header", "true").csv("iot_alerts")

print("\n Alerts → iot_alerts/")
print(" PIPELINE COMPLETE!")
spark.stop()
