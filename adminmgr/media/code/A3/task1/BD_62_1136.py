from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType,StructField,StringType,DoubleType
 
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

tweets = lines.select(explode(split(lines.Hashtags, ',')).alias('hashtags'))
#creation of table 
tweets.createOrReplaceTempView("tweets")
hashtag_counts_df = spark.sql("select Hashtags,count(Hashtags) as count from tweets group by Hashtags order by count desc limit 3")	
query=hashtag_counts_df.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(60)
query.stop()
