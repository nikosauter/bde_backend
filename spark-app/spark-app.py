from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructType, StringType
from pyspark.sql.functions import unix_timestamp, from_utc_timestamp, col, regexp_extract_all, split, array_distinct, lit, explode, window, to_timestamp

spark = SparkSession.builder \
    .appName("Spark Example") \
    .getOrCreate()


postSchema = StructType() \
    .add("id", LongType()) \
    .add("timestamp", StringType()) \
    .add("user", StringType()) \
    .add("text", StringType())

data = [
    (1467810672, "2023-12-20 14:30:45", "scotthamilton", "is upset that he can't update his Facebook by texting it... and might cry as a result School today ..."),
    (1467810917, "2023-12-20 15:30:45", "mattycus", "@Kenichan I dived many times for the #ball. Managed to save 50% The rest go out of bounds"),
    (1467811184, "2023-12-18 16:30:45", "ElleCTF", "my whole #body feels itchy and like its on #fire"),
    (1467811184, "2023-12-18 16:30:45", "ElleCTF", "my whole #body feels itchy and like its on #fire"),
    (1467811184, "2023-12-18 16:30:45", "ElleCTF", "my whole #body feels itchy and like its on #fire"),
    (1467811184, "2023-12-20 16:30:45", "ElleCTF", "my whole #body feels itchy and like its on #fire"),
    (1467811190, "2023-12-20 16:30:45", "ElleCTF", "my whole #body feels itchy and like its on #fire"),
]
df = spark.createDataFrame(data, schema=postSchema).drop(col("id"), col("user"))

df = df \
    .withColumn("timestamp", to_timestamp(col("timestamp").cast("timestamp"))) \
    .withColumn("hashtags", regexp_extract_all('text', lit(r'(#\w+)'))) \
    .drop(col("text")) \
    .select(col("timestamp"), explode(col("hashtags"))) \
    .withColumnRenamed("col", "hashtag")

trendingTopics = df.groupBy(
    window(
        col("timestamp"),
        "1 minute",
        "1 minute"
    ),
    col("hashtag")
).count()

df.printSchema()

trendingTopics.orderBy(col("window").desc(), col("count").desc()).show()
