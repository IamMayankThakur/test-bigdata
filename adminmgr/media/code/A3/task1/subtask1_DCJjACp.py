from pyspark.sql import Row, SQLContext
from pyspark import SparkConf, SparkContext
from operator import add
import requests
import sys
from pyspark.streaming import StreamingContext
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode, split
from pyspark.sql.functions import col


findspark.init()

ssc = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

schema = StructType().add("ID", "string").add("Lang", "string") \
    .add("Date", "string").add("Source", "string").add("Len", "string") \
    .add("Likes", "string").add("RTs", "string").add("Hashtags", "string") \
    .add("UserMentionName", "string").add("UserMentionID", "string").add("name", "string") \
    .add("Place", "string").add("Followers", "float").add("Friends", "float")

lines = ssc \
    .readStream \
    .format("csv") \
    .option("header", True) \
    .schema(schema) \
    .option("sep", ";") \
    .csv('hdfs://localhost:9000/stream')

words = lines.select(
    explode(
        split(lines.Hashtags, ",")
    ).alias("Hashtags")
)


hash1 = words.groupBy("Hashtags").count().sort(col("count").desc())
hash1.createOrReplaceTempView("hashtable")
hash1 = ssc.sql("select * from hashtable limit 5")
hash1 = hash1.groupBy("Hashtags").count()
hash1=hash1.drop('count')

query = hash1 \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination(60)
query.stop()
