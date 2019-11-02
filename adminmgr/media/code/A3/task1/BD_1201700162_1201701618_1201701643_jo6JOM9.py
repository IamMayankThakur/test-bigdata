from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.types import *
import sys
import requests
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkcommonhashtag") \
    .getOrCreate()
# Create DataFrame representing the stream of input lines from connection to localhost:9999
userSchema = StructType() \
			.add("id", "string") \
			.add("lang","integer") \
			.add("Date","string") \
			.add("Source","string") \
			.add("len","string") \
			.add("Likes","string") \
			.add("RTs.","string") \
			.add("Hashtags","string") \
			.add("UserMentionNames","string") \
			.add("UserMentionID","string") \
			.add("Name","string") \
			.add("Place","string") \
			.add("Followers","integer") \
			.add("Friends","integer")

lines = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream")

# Split the lines into words
words = lines.select(explode(split(lines.Hashtags,",")).alias("Hashtags"))
wordCounts=words.groupBy("Hashtags").count().orderBy(desc("count")).limit(5)

query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination(60)
query.stop()
