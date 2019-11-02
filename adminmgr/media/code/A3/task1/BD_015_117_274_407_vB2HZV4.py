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
from pyspark.sql.functions import col, desc, asc



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
    ).alias("Hashtags"), "ID"
)

# lines.createOrReplaceTempView("frr")

hash1 = words.groupBy("Hashtags").count().sort(desc("count"),asc("Hashtags"))
hash1.createOrReplaceTempView("hashtable")
hash1 = ssc.sql("select * from hashtable limit 5")

# frr = ssc.sql("select * from frr ")
# frr = lines.withColumn('FRRatio', frr.Followers/frr.Friends)
# frr=frr.groupBy((['name', frr.Followers, frr.Friends,frr.FRRatio])).count().sort(col("FRRatio").desc())
# frr = frr.drop("count")
# frr = frr.drop("Followers")
# frr = frr.drop("Friends")

# frr.createOrReplaceTempView("frr1")
# frr = ssc.sql("select * from frr1 limit 5")


query = hash1 \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination(100)
query.stop()
