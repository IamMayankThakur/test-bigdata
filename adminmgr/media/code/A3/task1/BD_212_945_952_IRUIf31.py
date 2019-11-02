import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import explode, split
import sys
import requests

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark = SparkSession.builder.appName("BigDataA3").getOrCreate()

# Define schema of the csv
userSchema = StructType().add("id","string").add("lang","string").add("date","string").add("source","string").add("len","integer").add("likes","integer").add("rts","string" ).add("hashtags","string")\
                   .add("usermentionnames","string" ).add("usermentionid","string").add("name","string").add("place","string" ).add("followers","integer").add("friends","integer")

# Read CSV files from set path
dfCSV = spark.readStream.option("sep", ";").option("header", "false").schema(userSchema).csv("/stream")

dfCSV.createOrReplaceTempView("fifa")

dfCSV1 = dfCSV.select(explode(split(dfCSV.hashtags, ",")).alias("Hashtags"))
dfCSV1.createOrReplaceTempView("df")

hashtag = spark.sql("select Hashtags,count(*) as count from df group by Hashtags order by count desc limit 5")
#hashtag = spark.sql("select * from h1")
#print(userSchema)
query = hashtag.writeStream.outputMode("complete").format("console").start()

query.awaitTermination(100)
query.stop()

