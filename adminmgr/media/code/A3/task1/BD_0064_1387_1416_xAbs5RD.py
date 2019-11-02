from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import csv
from pyspark.sql.functions import col,split,explode
#from pyspark.sql.GroupedData import max
	
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
words = row.select(
   explode(
       split(row.ht, ",")
   ).alias("Hashtags")
)

words.createOrReplaceTempView("updates")
op = spark.sql("select Hashtags,count(*) as count from updates group by Hashtags order by count desc LIMIT 5")

query =  op \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination(100)
query.stop()

