from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, from_json, window, count

spark = SparkSession.builder \
        .appName("ClickStreamAnalyticsEngine") \
        .config("spark.jars.package", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "4")


schema = StructType([
    StructField("user_id", StringType(), nullable = False),
    StructField("session_id", StringType(), nullable = False),
    StructField("event_time", TimestampType(), nullable = False),
    StructField("page_url", StringType(), nullable = False),
    StructField("action", StringType(), nullable = False),
    StructField("ip_address", StringType(), nullable = False),
])

raw_kafka_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "127.0.0.1:9094") \
                .option("subscribe", "clickstream") \
                .option("startingOffsets", "latest") \
                .load()


parsed_df = raw_kafka_df \
            .selectExpr("CAST(value as STRING) as json_payload") \
            .select(from_json(col("json_payload"), schema).alias("data")) \
            .select("data.*")


watermarked_df = parsed_df.withWatermark("event_time", "10 minutes")

session_metrics_df = watermarked_df.groupBy( 
    window(col("event_time"), "10 minutes"),
    col("session_id")
).agg(count("action")).alias("total_clicks")

bot_anomalies_df = (watermarked_df.groupBy( 
    window(col("event_time"), "1 minute"),
    col("ip_address")
).agg(count("action").alias("total_clicks_per_minute")). 
    filter(col("total_clicks_per_minute") > 100))




