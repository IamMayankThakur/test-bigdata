import sys
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType,StructField,StringType,DoubleType
spark = SparkSession\
	.builder\
	.appName("Assgn3")\
	.getOrCreate()
schem = StructType([   StructField("id",StringType() , True), 
			StructField("language", StringType(), True),
			StructField("date", StringType(), True),
			StructField("source", StringType(), True),
			StructField("len", StringType(), True),
			StructField("likes",StringType(), True), 
			StructField("RTs", StringType(), True),
			StructField("hashtags",StringType(), True),
			StructField("usermentionnames", StringType(), True),
			StructField("usermentionid", StringType(), True), 
			StructField("name", StringType(), True),
			StructField("place", StringType(), True),
			StructField("followers", DoubleType(), True),
			StructField("friends",DoubleType() , True),])
l = spark\
	.readStream\
	.option("sep",";")\
	.schem(schem)\
	.csv("hdfs://localhost:8000/stream")
d = l.select(explode(lines.Hashtags).alias("hashtags"))
d.createOrReplaceTempView("d")
c_value = spark.sql("select hashtags , count(hashtags) as COUNT from d group by Hashtags order by COUNT desc limit 5")
output=c_value.writeStream.outputMode("complete").format("console").start()
output.awaitTermination(60)
output.stop()
