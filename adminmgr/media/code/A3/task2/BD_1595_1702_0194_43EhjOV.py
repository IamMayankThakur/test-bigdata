from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import csv
from pyspark.sql.functions import col,split,explode


	
sp = SparkSession.builder.appName("app").getOrCreate()

myschema = StructType().add("Id", "string").\
	add("lang", "string").\
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

rows = sp.readStream.option("sep", ";").schema(myschema).csv("/stream")
rows.createOrReplaceTempView("updates")
out =  sp.sql("select name,max(followers/friends) as FRRatio from updates group by name order by  FRRatio desc LIMIT 1")



query =  out.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(100)
query.stop()
