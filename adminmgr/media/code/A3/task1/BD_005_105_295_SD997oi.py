import findspark
findspark.init()
import pyspark
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
from pyspark.sql import Row,SQLContext
from operator import add
import requests
import time
import pandas as pd
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import lit


userSchema = StructType().add("ID", "string").add("Lang", "string").add("Date","string").add("Source","string").add("len","integer").add("likes","string").add("RTs","string").add("Hashtags",
"string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","string").add("Friends","string")

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("/stream")



#w2=w1.rdd
csvDF.createOrReplaceTempView("updates")



#wordCounts=spark.sql("select Hashtags, count(*) as count from updates group by Hashtags order by count desc limit 1")
#wordCounts=wordCounts.rdd

w1=csvDF.select(explode(split(csvDF.Hashtags,",")).alias("hashtags"))
w1.createOrReplaceTempView("updates1")
w2=spark.sql("select hashtags as Hashtags ,count(*) as count from updates1 group by hashtags order by count(*) desc limit 5")






 # Start running the query that prints the running counts to the console
query = w2 \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

 

query.awaitTermination(100)
query.stop() 
#time.sleep(10)





