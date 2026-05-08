import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType

# Define event schema matching Avro format
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
    StructField("payload", MapType(StringType(), StringType()), True)
])

def bot_detector(ua_string):
    if not ua_string:
        return "false"
    ua_lower = ua_string.lower()
    bots = ["bot", "crawler", "spider", "scrape", "headless"]
    for bot in bots:
        if bot in ua_lower:
            return "true"
    return "false"

def simple_geoip_lookup(ip):
    # Simulated high-performance lookup function instead of external REST call
    if not ip:
        return "Unknown"
    octets = ip.split('.')
    if len(octets) != 4:
        return "Unknown"
    # Map first octet to simulated countries
    first = int(octets[0]) % 5
    mapping = {0: "US", 1: "GB", 2: "IN", 3: "DE", 4: "CA"}
    return mapping.get(first, "US")

bot_detector_udf = udf(bot_detector, StringType())
geoip_udf = udf(simple_geoip_lookup, StringType())

def main():
    spark = SparkSession.builder \
        .appName("ClickstreamEnrichmentStream") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    # Read clickstream from raw topic
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "clickstream.raw") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON payloads (assuming JSON serialization for base demo compatibility)
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), EVENT_SCHEMA).alias("data")) \
        .select("data.*")

    # Apply Enrichments
    df_enriched = df_parsed \
        .withColumn("is_bot", bot_detector_udf(col("user_agent"))) \
        .withColumn("country_code", geoip_udf(col("ip_address"))) \
        .withColumn("ingestion_timestamp", expr("current_timestamp()"))

    # Write enriched events back to Kafka enriched topic
    query = df_enriched \
        .selectExpr("CAST(user_id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "clickstream.enriched") \
        .option("checkpointLocation", "/tmp/spark_checkpoints/enrichment") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()

# Kafka source mapping

# Deserialization schemas
