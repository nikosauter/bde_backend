from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructType, StringType
from pyspark.sql.functions import expr, from_unixtime, unix_timestamp, from_utc_timestamp, col, regexp_extract_all, split, array_distinct, lit, explode, window, to_timestamp

spark = SparkSession.builder \
    .appName("Spark Example") \
    .getOrCreate()

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
timestamp_format = "EEE MMM dd HH:mm:ss zzz yyyy"

postSchema = StructType() \
    .add("id", LongType()) \
    .add("timestamp_str", StringType()) \
    .add("user", StringType()) \
    .add("text", StringType())

data = [
    (1467810672, "Mon Jun 01 12:44:55 PDT 2009", "scotthamilton", "is upset that he can't update his Facebook by texting it... and might cry as a result School today ..."),
    (1467810917, "Mon Jun 01 12:44:55 PDT 2009", "mattycus", "@Kenichan I dived many times for the #ball. Managed to save 50% The rest go out of bounds"),
    (1467811184, "Mon Jun 01 12:44:55 PDT 2009", "ElleCTF", "my whole #body feels itchy and like its on #fire"),
    (1467811184, "Mon Jun 01 12:44:55 PDT 2009", "ElleCTF", "my whole #body feels itchy and like its on #fire"),
    (1467811184, "Mon Jun 01 12:44:55 PDT 2009", "ElleCTF", "my whole #body feels itchy and like its on #fire"),
    (1467811184, "Mon Jun 01 12:44:55 PDT 2009", "ElleCTF", "my whole #body feels itchy and like its on #fire"),
    (1467811190, "Mon Jun 01 12:44:55 PDT 2009", "ElleCTF", "my whole #body feels itchy and like its on #fire"),
]
df = spark.createDataFrame(data, schema=postSchema).drop(col("id"), col("user"))

df = df \
    .withColumn("unix_timestamp", unix_timestamp("timestamp_str", timestamp_format)).withColumn("parsed_timestamp", from_unixtime("unix_timestamp")).withColumn("subtracted_timestamp", expr("parsed_timestamp - interval 9 hours")) \
    .withColumn("hashtags", regexp_extract_all('text', lit(r'(#\w+)'))) \
    .drop(col("text")) \
    .select(col("parsed_timestamp"), explode(col("hashtags"))) \
    .withColumnRenamed("col", "hashtag")

df.show(10)

trendingTopics = df.groupBy(
    window(
        col("parsed_timestamp"),
        "1 minute",
        "1 minute"
    ),
    col("hashtag")
).count()

df.printSchema()

trendingTopics.orderBy(col("window").desc(), col("count").desc()).show()
