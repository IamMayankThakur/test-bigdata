import findspark
findspark.init()
import pyspark
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
from operator import add
import requests
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


userSchema = StructType().add("ID", "string").add("Lang", "string").add("Date","string").add("Source","string").add("len","integer").add("likes","string").add("RTs","string").add("Hashtags",
"string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","float").add("Friends","float")

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkCount") \
    .getOrCreate()

csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream")

csvDF.createOrReplaceTempView("updates")
wordCounts=spark.sql("select Name,Followers/Friends as ratio from updates")
wordCounts.createOrReplaceTempView("updates1")
w1=spark.sql("select Name as name , max(ratio) as FRRatio from updates1 group by Name order by max(ratio) desc limit 5")
#w1=spark.sql("select max(ratio) from updates1")
#row_rdd = csvDF.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
#row_rdd = row_rdd.sortBy(lambda x: Row(x[0]),False)
#for (i,j) in row_rdd:


 # Start running the query that prints the running counts to the console
query = w1 \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

 

query.awaitTermination(60)
query.stop()
#spark.awaitTermination()




