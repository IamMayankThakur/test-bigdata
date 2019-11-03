import findspark
findspark.init()

import sys
import requests
#import time
import pandas as pd
from operator import add

from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import split, explode
from pyspark.sql.types import StructType



Schema = StructType().add("ID", "string").add("Lang", "string").add("Date","string").add("Source","string").add("len","integer").add("likes","string").add("RTs","string").add("Hashtags","string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","string").add("Friends","string")

spark = SparkSession \
    .builder \
    .appName("MostCommonHashtag") \
    .getOrCreate()

DF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(Schema) \
    .csv("hdfs://localhost:9000/stream")



split = DF.select(explode(split(DF.Hashtags,",")).alias("Hashtags"))

split.createOrReplaceTempView("updates")

new = split.groupBy("Hashtags").count().alias("count").orderBy("count",ascending=False)

var = new.limit(5)

query = var \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

 

query.awaitTermination(60)
query.stop()
