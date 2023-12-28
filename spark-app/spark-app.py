from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructField, StructType, StringType
from pyspark.sql.functions import (
    column, from_json, expr, from_unixtime, unix_timestamp, regexp_extract_all,
    explode, window, col, lit, to_timestamp
)

def save_to_database(batch_dataframe, batch_id):
    global db_url, db_schema, db_options
    print(f"Writing batchID {batch_id} to database @ {db_url}")
    batch_dataframe.distinct().write.jdbc(db_url, db_schema, "overwrite", db_options)

db_url = 'jdbc:mysql://my-app-mariadb-service:3306/user_posts'
db_options = {"user": "root", "password": "mysecretpw"}
db_schema = 'trending_hashtags'

spark = SparkSession.builder \
    .appName("Spark Example") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
timestamp_format = "EEE MMM dd HH:mm:ss zzz yyyy"

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

transformed_posts = post_stream.select(
     from_json(
         column("value").cast("string"),
         post_schema
     ).alias("json")
).select(
     column("json.*")
)

hashtag_occurrences = transformed_posts \
     .withColumn("unix_timestamp", unix_timestamp("timestamp", timestamp_format)) \
     .withColumn("parsed_timestamp", to_timestamp("unix_timestamp")) \
     .withColumn("hashtags", regexp_extract_all('text', lit(r'(#\w+)'))) \
     .drop("text") \
     .select(col("parsed_timestamp"), explode(col("hashtags")).alias("hashtag"))

aggregated_posts = hashtag_occurrences.groupBy(
     window(col("parsed_timestamp"), "1 hour", "10 minutes"),
     col("hashtag")
).count() \
    .withColumn("post_window_start", col("window.start")) \
    .withColumn("post_window_end", col("window.end")) \
    .drop("window", "post_window_start")

console_dump = aggregated_posts \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

db_insert_stream = aggregated_posts \
    .select(column("post_window_end"), column("hashtag"), column('count').alias("counter")) \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(save_to_database) \
    .start()

spark.streams.awaitAnyTermination()
