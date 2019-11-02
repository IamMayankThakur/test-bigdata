# Assignment 3 task-1 part 2, Big data [UE17CS313], PES University

'''
Spark streaming using pyspark to find out the most popular user from the given dataset.
The dataset (fifa_modded_small_1.csv) comprises of collection of tweets during 2018 FIFA world cup.
The Columns of the dataset are:
ID,Lang,Date,Source,len,Likes,RT's,Hashtags,UserMentionNames,UserMentionID,Name,Place,Followers,Friends
The file is being read from /stream directory on HDFS.
'''
# import files
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType
from pyspark.sql.functions import split, explode, col
from pyspark.sql.functions import *

# Create a spark session. handle name -> spark
spark = SparkSession.builder.appName("Streaming-popularUser").getOrCreate()

spark = SparkSession.builder.master("local").appName("test-mf").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define schema of csv
userSchema = StructType()\
	.add("ID","string")\
	.add("Lang","string")\
	.add("Date","string")\
	.add("Source","string")\
	.add("Len","integer")\
	.add("Likes","integer")\
	.add("RT","integer")\
	.add("Hashtags","string")\
	.add("MentionNames","string")\
	.add("MentionID","string")\
	.add("Name","string")\
	.add("Place","string")\
	.add("Followers","integer")\
	.add("Friends","integer")	

# Read CSV from HDFS path.schema is the above defined schema.
dfCSV = spark.readStream.option("sep",";").option("header","false").schema(userSchema).csv("hdfs://localhost:9000/stream/")

# Creates temporary view for the given name
dfCSV.createOrReplaceTempView("popularUser")

# SQL query on the data

popularity = spark.sql("select Name as name, Followers/Friends as FRRatio from popularUser")

#temp = popularity.orderBy(desc("FRRatio"))

temp = popularity.groupBy("name","FRRatio").count().orderBy(col("FRRatio").desc()).limit(1)

#print("dfCSV: ",type(dfCSV))
#print("temp: ",type(temp))

temp.createOrReplaceTempView("temp")

final = spark.sql("select name,FRRatio from temp")

# Start spark standard streaming query.
query = final.writeStream.outputMode("complete").format("console").start()

#Terminate after 100 seconds
query.awaitTermination(100)

