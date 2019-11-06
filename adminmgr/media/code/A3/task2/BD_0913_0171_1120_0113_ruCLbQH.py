from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split,max,struct
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType
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
			StructField("followers",DoubleType(), True),
			StructField("friends",DoubleType() , True),])

lines = spark\
	.readStream\
	.option('sep',";")\
	.schema(schema)\
	.csv("hdfs://localhost:9000/stream")

follower = lines.followers
friend = lines.friends
names = lines.name

d1 = lines.select(lines.name,((follower)/(friend)).alias("FRRatio"))
d1.createOrReplaceTempView("d1")
count = spark.sql("select name,sum(FRRatio) as FRRatio from d1 group by name order by FRRatio desc limit 1")
proc=count.writeStream.outputMode("complete").format("console").start()
proc.awaitTermination(60)
proc.stop()


