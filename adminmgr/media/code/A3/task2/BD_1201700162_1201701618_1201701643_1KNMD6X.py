from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.types import *
from operator import add
import sys
import requests
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkcommon") \
    .getOrCreate()
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
			.add("name","string") \
			.add("Place","string") \
			.add("Followers","integer") \
			.add("Friends","integer")
lines = spark \
    .readStream \
	.option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream/")
# Split the lines into words
words = lines.select("name",(col("Followers")/col("Friends")).alias("FRRatio"))
maximum=words.groupBy("name").agg(sum(col("FRRatio")).alias('FRRatio')).orderBy(desc("FRRatio")).limit(1)

query = maximum.writeStream \
	.outputMode("complete") \
	.format("console") \
	.start() 
query.awaitTermination(60)
query.stop()


