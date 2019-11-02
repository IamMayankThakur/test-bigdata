#import findspark
#findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,SparkSession,GroupedData
from pyspark.sql.functions import count,split,explode,desc
from pyspark.sql.types import StructType

import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

spark = SparkSession \
	.builder \
	.appName("BD_057_214_659_1799") \
	.getOrCreate()

userSchema = StructType().add("ID","string").add("language","string").add("date","string").add("source","string").add("len","integer").add("likes","integer").add("RT","integer").add("hashtags","string").add("username","string").add("userid","string").add("name","string").add("place","string").add("followers","integer").add("friends","integer")
csvDF = spark \
	.readStream \
	.option("sep",";") \
	.schema(userSchema) \
	.csv("/stream/")


newdf= csvDF.select(explode(split(csvDF.hashtags,",")).alias("Hashtags"),"ID")
countdf = newdf.groupBy(newdf.Hashtags).count()
finaldf = countdf.select("*").orderBy(desc("count")).limit(5)

query = finaldf.writeStream \
	.outputMode("complete") \
	.format("console") \
	.start()
query.awaitTermination(100)
query.stop()


