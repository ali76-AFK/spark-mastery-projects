#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Social-Media-Trends").getOrCreate()

# Load 1.44M social posts
df = spark.read.option("header", "true").csv("social_media_posts.csv")
df = df.withColumn("likes", col("likes").cast("int")) \
       .withColumn("retweets", col("retweets").cast("int")) \
       .withColumn("follower_count", col("follower_count").cast("double"))

print("📱 Dataset: {:,} social posts".format(df.count()))
df.select("hashtags", "likes", "retweets", "is_bot").describe().show()

# Trend detection stats
stats = df.agg(
    mean("likes").alias("likes_mean"),
    stddev("likes").alias("likes_std"),
    mean("retweets").alias("rt_mean"),
    sum(col("is_bot").cast("int")).alias("bot_count")
).collect()[0]

print("🔥 Engagement Stats:")
print(f"Likes: μ={stats['likes_mean']:.0f}±{stats['likes_std']:.0f}")
print(f"Bots: {stats['bot_count']/df.count()*100:.1f}%")

# Viral trends + bot detection
df_trends = df.withColumn("engagement_score", 
    (col("likes") * 0.7 + col("retweets") * 0.3) / (col("follower_count") + 1)
).withColumn("likes_zscore", 
    abs((col("likes") - stats["likes_mean"]) / stats["likes_std"])
).filter(
    (col("likes_zscore") > 3) | 
    (col("is_bot") == True) | 
    (col("engagement_score") > stats["likes_mean"] * 2)
)

print("🚨 VIRAL TRENDS + BOTS: {:,} alerts ({:.1f}%)".format(
    df_trends.count(), df_trends.count()/df.count()*100))

print("🏆 Top 10 viral trends:")
df_trends.filter(col("likes_zscore") > 3).select(
    "timestamp", "user_id", "hashtags", "likes", "retweets", "likes_zscore"
).orderBy(col("likes_zscore").desc()).limit(10).show(truncate=False)

# Hashtag ranking
hashtag_trends = df.withColumn("hashtag", explode(split(col("hashtags"), '\\|'))) \
    .groupBy("hashtag").agg(
        count("*").alias("post_count"),
        avg("likes").alias("avg_likes"),
        sum("likes").alias("total_engagement")
    ).orderBy(col("total_engagement").desc())

print("\n📈 Top Hashtags:")
hashtag_trends.show(10, truncate=False)

df_trends.coalesce(1).write.mode("overwrite").option("header", "true").csv("social_alerts")
print("💾 Alerts → social_alerts/part-*.csv")

spark.stop()
