#!/usr/bin/python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_session_object = SparkSession.builder\
    .appName('StructuredSparkStreamingHadoop')\
    .getOrCreate()


sch = StructType().add("Id", "integer").add("Lang", "string")\
                .add("Date", "string").add("Source", "string").add("len", "integer")\
                .add("Likes", "integer").add("RTs", "integer").add("Hashtags", "string")\
                .add("UserMentionNames", "string").add("UserMentionID", "string")\
                .add("Name", "string").add("Place", "string").add("Followers", "integer")\
                .add("Friends","integer")

ld = spark_session_object.readStream\
    .option('sep', ';')\
    .option('header', 'false')\
    .schema(sch)\
    .csv('hdfs://localhost:9000/stream/')

wd = ld.select('Hashtags')

wdf = wd.groupBy('Hashtags')
wdf1=wdf.count()
wdf2=wdf1.orderBy(desc('count')).limit(5)

query_object = wdf2.select("Hashtags","count").writeStream\
    .outputMode('complete').format('console')

query_runner = query_object.start()
query_runner.awaitTermination(60)
query_runner.stop()




