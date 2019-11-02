from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import csv
from pyspark.sql.functions import col,split,explode
#from pyspark.sql.GroupedData import max

def convert(x):
	return int(x)
	
	
	
spark = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()
lines = spark.readStream.format("socket").option("host","localhost").option("port", 9000).load()
#print(lines.isStreaming)

schema = StructType().add("Id", "string").add("lang", "string").\
        add("date", "string").\
        add("source", "string").\
	add("len","integer").\
	add("likes", "integer").\
	add("rt", "integer").\
	add("ht", "string").\
	add("umn", "string").\
	add("umi", "string").\
	add("name", "string").\
	add("place", "string").\
	add("followers", "integer").\
	add("friends","integer")

row = spark \
    .readStream \
    .option("sep", ";") \
    .schema(schema) \
    .csv("hdfs://localhost:9000/stream/")

table = row.select(row.name,row.followers,row.friends)



table.createOrReplaceTempView("updates")
op =  spark.sql("select name,sum(cast(followers as double))/sum(cast(friends as bigint)) as FRRatio from updates group by name order by  FRRatio desc LIMIT 1")



query =  op \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination(60)
query.stop()
