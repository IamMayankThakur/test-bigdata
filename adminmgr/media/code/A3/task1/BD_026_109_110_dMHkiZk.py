from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType,StructField,StringType,DoubleType

import sys

spark = SparkSession\
	.builder\
	.appName("fifa")\
	.getOrCreate()

schema = StructType([   StructField("id",StringType() , True), 
			StructField("language", StringType(), True),
			StructField("date", StringType(), True),
			StructField("source", StringType(), True),
			StructField("len", StringType(), True),
			StructField("likes",StringType(), True), 
			StructField("RTs", StringType(), True),
			StructField("Hashtags",StringType(), True),
			StructField("usermentionnames", StringType(), True),
			StructField("usermentionid", StringType(), True), 
			StructField("name", StringType(), True),
			StructField("place", StringType(), True),
			StructField("followers", DoubleType(), True),
			StructField("friends",DoubleType() , True),])

lines = spark\
	.readStream\
	.option('sep',";")\
	.schema(schema)\
	.csv("hdfs://localhost:9000/stream")

data_twitter = lines.select(explode(lines.Hashtags).alias('hashtags'))
data_twitter.createOrReplaceTempView("data_twitter")
count = spark.sql("select Hashtags,count(Hashtags) as count from data_twitter group by Hashtags order by count desc limit 5")

proc=count.writeStream.outputMode("complete").format("console").start()
proc.awaitTermination(60)
proc.stop()
