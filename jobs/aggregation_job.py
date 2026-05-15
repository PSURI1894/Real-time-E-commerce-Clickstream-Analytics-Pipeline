import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, count, sum, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("client_timestamp", LongType(), True),
    StructField("server_timestamp", LongType(), True),
    StructField("ip_address", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("payload", MapType(StringType(), StringType()), True),
    StructField("is_bot", StringType(), True),
    StructField("country_code", StringType(), True)
])

def main():
    spark = SparkSession.builder \
        .appName("ClickstreamWindowedAggregations") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    # Read from clickstream.enriched topic
    df_enriched = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "clickstream.enriched") \
        .load()

    # Parse and enforce schemas
    df_parsed = df_enriched.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), EVENT_SCHEMA).alias("data")) \
        .select("data.*") \
        .filter(col("is_bot") == "false") # Filter bot traffic

    # Convert client timestamp (millis) to Timestamp type for Watermarking
    df_with_ts = df_parsed.withColumn("event_time", to_timestamp(col("client_timestamp") / 1000))

    # Watermark: allow up to 5 minutes lateness
    df_watermarked = df_with_ts.withWatermark("event_time", "5 minutes")

    # Aggregate by 1-min Tumbling Window for product pageviews, cart adds, purchases
    df_product_aggs = df_watermarked \
        .groupBy(
            col("payload").getItem("sku").alias("product_id"),
            window(col("event_time"), "1 minute")
        ) \
        .agg(
            count(when(col("event_type") == "page_view", 1)).alias("page_views_count"),
            count(when(col("event_type") == "add_to_cart", 1)).alias("adds_to_cart_count"),
            count(when(col("event_type") == "checkout", 1)).alias("purchases_count")
        ) \
        .select(
            col("product_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("page_views_count"),
            col("adds_to_cart_count"),
            col("purchases_count")
        )

    # In production, we write to Cassandra using org.apache.spark.sql.cassandra sink format.
    # For local runner demonstration, we'll write to console and verify pipeline connectivity
    query = df_product_aggs.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("checkpointLocation", "/tmp/spark_checkpoints/aggregations") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()

# Tumbling windows
