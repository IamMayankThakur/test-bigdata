from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split,max,struct
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType
 
spark = SparkSession\
	.builder\
	.appName("BigData")\
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
	.csv("hdfs://localhost:8080/stream")

tags=lines.select("Hashtags")
h = tags.select(
   explode(
       split("Hashtags", ",")
   ).alias("Hashtags")
)
c=h.groupby("Hashtags").count()
comm=c.select("Hashtags","count").orderBy("count",ascending=False).limit(5)
final_output=comm.writeStream.outputMode("complete").format("console").start()
final_output.awaitTermination(60)
final_output.stop()




