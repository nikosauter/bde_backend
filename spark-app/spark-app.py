from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructField, StructType, StringType
from pyspark.sql.functions import (
    column, from_json, expr, from_unixtime, unix_timestamp, regexp_extract_all,
    explode, window, col, lit
)

spark = SparkSession.builder \
    .appName("Spark Example") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
timestamp_format = "EEE MMM dd HH:mm:ss zzz yyyy"

postSchema = StructType([
    StructField("id", LongType()),
    StructField("timestamp", StringType()),
    StructField("user", StringType()),
    StructField("text", StringType())
])

postStream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-cluster:9092") \
    .option("subscribe", "posts") \
    .option("startingOffsets", "earliest") \
    .load()

# posts = postStream.select(
#     from_json(
#         column("value").cast("string"),
#         postSchema
#     ).alias("json")
# ).select(
#     column("json.*")
# )
#
# df = posts \
#     .withColumn("unix_timestamp", unix_timestamp("timestamp", timestamp_format)) \
#     .withColumn("parsed_timestamp", from_unixtime("unix_timestamp")) \
#     .withColumn("subtracted_timestamp", expr("parsed_timestamp - INTERVAL 9 HOURS")) \
#     .withColumn("hashtags", regexp_extract_all('text', lit(r'(#\w+)'))) \
#     .drop("text") \
#     .select(col("parsed_timestamp"), explode(col("hashtags")).alias("hashtag"))
#
# trendingTopics = df.groupBy(
#     window(col("parsed_timestamp"), "1 minute", "1 minute"),
#     col("hashtag")
# ).count()

consoleDump = postStream \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

spark.streams.awaitAnyTermination()
