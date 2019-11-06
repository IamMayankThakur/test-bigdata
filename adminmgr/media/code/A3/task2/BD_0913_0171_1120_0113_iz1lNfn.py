from __future__ import print_function
from pyspark.sql import SparkSession
#from pyspark.sql.functions import explode
from pyspark.sql.functions import split,max,struct
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType
import sys
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
			StructField("Name", StringType(), True),
			StructField("place", StringType(), True),
			StructField("followers",DoubleType(), True),
			StructField("friends",DoubleType() , True),])
lines = spark\
	.readStream\
	.option('sep',";")\
	.schema(schema)\
	.csv("hdfs://localhost:8080/stream")
all_data = lines.select(lines.name,((lines.followers)/(lines.friends)).alias("f_ratio"))
all_data.createOrReplaceTempView("all_data")
c = spark.sql("select Name,sum(f_ratio) as FRRatio from data_twitter group by Name order by FRRatio desc limit 1")
proc=c.writeStream.outputMode("complete").format("console").start()
proc.awaitTermination(60)
proc.stop()


