import re

from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import LongType, StructField, StructType, StringType, ArrayType
from pyspark.sql.functions import (
    column, from_json, expr, from_unixtime, unix_timestamp,
    explode, window, col, lit, to_timestamp, udf, lower
)

kafka_bootstrap_server = "kafka-cluster:9092"
kafka_topic = "posts"

db_service = "my-app-mariadb-service:3306"
db_url = f"jdbc:mysql://{db_service}/user_posts"
db_name = "user_posts"
db_user = "root"
db_password = "mysecretpw"
db_schema = 'trending_hashtags'

spark = SparkSession.builder \
    .appName("Spark Example") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
timestamp_format = "EEE MMM dd HH:mm:ss zzz yyyy"

sqlalchemy_engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_service}/{db_name}')

def upsert_into_database(df: DataFrame, epoch_id: int):
    # This function is a workaround for an undesirable behavior of the spark jdbc save mode "overwrite"
    # -> https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/SaveMode.html
    # As we want to have historic data of trending topic in the past, we end up having many rows to insert.
    # When writing a batch to a database in "overwrite" save mode, the table is first cleared.
    # After that, the spark job begins to insert the batch little by little. Because of that behavior,
    # there are significant time ranges within which the database has no or only a subset of data.
    # To prevent that, we first write the batch data to a temporary table in "overwrite" save mode.
    # The data is then upserted (inserted or updated) to the actual database table.

    print(f"Start writing batch {epoch_id} to database")
    temp_table_name = f"{db_schema}_temp_table"  # temporary table name

    # Write data into temporary table
    df.write.format("jdbc").options(
        url=db_url,
        driver="com.mysql.cj.jdbc.Driver",
        user=db_user,
        password=db_password,
        dbtable=temp_table_name,
    ).mode("overwrite").save()

    # Upsert data to actual database
    with sqlalchemy_engine.begin() as connection:
        connection.execute(text(
            """
                INSERT INTO trending_hashtags (window_start, window_end, hashtag, counter)
                SELECT window_start, window_end, hashtag, counter FROM {}
                ON DUPLICATE KEY UPDATE
                counter=VALUES(counter)
            """.format(temp_table_name)
        ))
    print(f"Finished writing batch {epoch_id} to database")

def extract_hashtags(text):
    hashtags = re.findall(r'\#\w+', text)
    lowercase_hashtags = [hashtag.lower() for hashtag in hashtags]
    return lowercase_hashtags

extract_hashtags_udf = udf(extract_hashtags, ArrayType(StringType()))

post_schema = StructType([
    StructField("id", LongType()),
    StructField("timestamp", StringType()),
    StructField("username", StringType()),
    StructField("text", StringType())
])

######################
### Spark Pipeline ###
######################

# Read messages from kafka topic (starts with first message available)
post_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# transform json to columns
stream_data = post_stream.select(
     from_json(
         column("value").cast("string"),
         post_schema
     ).alias("json")
).select(
     column("json.*")
)

# Convert timestamp string to actual timestamp
stream_data = stream_data \
    .withColumn("timestamp", unix_timestamp("timestamp", timestamp_format)) \
    .withColumn("timestamp", to_timestamp("timestamp"))

# Extract hashtags from every post and save them as String Array in separate column
stream_data = stream_data \
    .withColumn("hashtags", extract_hashtags_udf(col("text")))

# Explode hashtags column: One row for every hashtag in the hashtag String array
stream_data = stream_data \
    .select(col("id"), col("timestamp"), col("username"), explode(col("hashtags")).alias("hashtag"))

# Aggregate hashtags based on event-time. Ignore late date with a delay of at least 1 hour
stream_data = stream_data.withWatermark("timestamp", "1 hour") \
    .groupBy(
        window(col("timestamp"), "24 hours", "24 hours"),
        col("hashtag")
    ) \
    .count()

# Convert window column (struct) to two separate columns
stream_data = stream_data \
    .withColumnRenamed("count", "counter") \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

# Save batches in database
db_insert_stream = stream_data \
    .select(column("window_start"), column("window_end"), column("hashtag"), column("counter")) \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(upsert_into_database) \
    .start()

spark.streams.awaitAnyTermination()
