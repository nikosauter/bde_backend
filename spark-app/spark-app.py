import re

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructField, StructType, StringType, ArrayType
from pyspark.sql.functions import (
    column, from_json, expr, from_unixtime, unix_timestamp,
    explode, window, col, lit, to_timestamp, udf
)

def save_to_database(batch_dataframe, batch_id):
    global db_url, db_schema, db_options
    print(f"Writing batchID {batch_id} to database @ {db_url}")
    batch_dataframe.distinct().write.jdbc(db_url, db_schema, "append", db_options)

db_url = 'jdbc:mysql://my-app-mariadb-service:3306/user_posts'
db_options = {"user": "root", "password": "mysecretpw"}
db_schema = 'trending_hashtags'

spark = SparkSession.builder \
    .appName("Spark Example") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
timestamp_format = "EEE MMM dd HH:mm:ss zzz yyyy"

def extract_hashtags(text):
    return re.findall(r'\#\w+', text)

extract_hashtags_udf = udf(extract_hashtags, ArrayType(StringType()))

post_schema = StructType([
    StructField("id", LongType()),
    StructField("timestamp", StringType()),
    StructField("username", StringType()),
    StructField("text", StringType())
])

post_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-cluster:9092") \
    .option("subscribe", "posts") \
    .option("startingOffsets", "earliest") \
    .load()

stream_data = post_stream.select(
     from_json(
         column("value").cast("string"),
         post_schema
     ).alias("json")
).select(
     column("json.*")
)

stream_data = stream_data \
    .withColumn("timestamp", unix_timestamp("timestamp", timestamp_format)) \
    .withColumn("timestamp", to_timestamp("timestamp"))

stream_data = stream_data.withColumn("hashtags", extract_hashtags_udf(col("text")))

stream_data = stream_data.select(col("id"), col("timestamp"), col("username"), explode(col("hashtags")).alias("hashtag"))

stream_data = stream_data.withWatermark("timestamp", "1 hour") \
    .groupBy(
        window(col("timestamp"), "24 hours", "3 hours"),
        col("hashtag")
    ) \
    .count()

stream_data = stream_data \
    .withColumnRenamed("count", "counter") \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

query = stream_data \
    .writeStream \
    .outputMode("complete") \
    .option("numRows", 40) \
    .option("truncate", False) \
    .format("console") \
    .start()

db_insert_stream = stream_data \
    .select(column("window_start"), column("window_end"), column("hashtag"), column("counter")) \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(save_to_database) \
    .start()

spark.streams.awaitAnyTermination()
