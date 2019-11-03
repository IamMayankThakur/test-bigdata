from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split,max,struct
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType
 
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
			StructField("followers",DoubleType(), True),
			StructField("friends",DoubleType() , True),])
lines=spark \
	.readStream \
	.option("sep",";") \
	.schema(schema) \
	.csv("hdfs://localhost:9000/stream")

tags=lines.select("Hashtags")
hashs = tags.select(
   explode(
       split("Hashtags", ",")
   ).alias("Hashtags")
)

count=hashs.groupby("Hashtags").count()

most_common=count.select("Hashtags","count").orderBy("count",ascending=False).limit(5)

query=most_common.writeStream.outputMode("complete").format("console").start()


query.awaitTermination(60)
query.stop()

