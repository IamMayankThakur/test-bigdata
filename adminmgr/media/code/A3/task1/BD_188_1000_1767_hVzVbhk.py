from pyspark.sql.functions import *
import re
import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark = SparkSession \
    .builder \
    .appName("task_1") \
    .getOrCreate()

def printfunc(x,y):
	a = x.rdd.cache()
	b = a.collect()
	print(b[0].hashtag)

# Read all the csv files written atomically in a directory
userSchema =StructType().add("ID","string").add("language","string").add("date","string").add("source","string").add("len","string").add("likes","string").add("RTs","string").add("Hashtags","string").add("Usernames","string").add("Userid","string").add("name","string").add("Place","string").add("followers","integer").add("freinds","integer")

csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/test1")  # Equivalent to format("csv").load("/path/to/directory")

temp = csvDF.select(explode(split(csvDF.Hashtags,",")).alias("Hashtags"))
temp = temp.groupBy('Hashtags').count().orderBy(desc('count'))
temp = temp.limit(5)

query = temp \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination(60)
query.stop()
#.foreachBatch(printfunc) \
#    .outputMode("complete") \
