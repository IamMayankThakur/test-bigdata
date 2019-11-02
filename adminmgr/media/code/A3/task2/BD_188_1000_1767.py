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
	print(type(x))
	a = x.rdd.cache()
	print(a.collect())

# Read all the csv files written atomically in a directory
userSchema =StructType().add("ID","string").add("language","string").add("date","string").add("source","string").add("len","string").add("likes","string").add("RTs","string").add("Hashtags","string").add("Usernames","string").add("Userid","string").add("name","string").add("Place","string").add("followers","integer").add("friends","integer")

csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/test1")  # Equivalent to format("csv").load("/path/to/directory")

temp = csvDF.select("name","followers","friends")
temp = temp.select("name",(temp.followers/temp.friends).alias("FRRatio"))
temp = temp.groupBy("name","FRRatio").count().orderBy(desc("FRRatio"))
temp = temp.select("name","FRRatio")
temp = temp.limit(1)
#temp = temp.groupBy('hashtag').count().orderBy(desc('count'))

temp \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start() \
    .awaitTermination(50)
#.foreachBatch(printfunc) \
#    .outputMode("complete") \
