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

dfCSV1 = dfCSV.select(dfCSV.name,(dfCSV.followers/dfCSV.friends).alias("FRRatio"))
dfCSV1.createOrReplaceTempView("df")

#hashtag = spark.sql("select name,FRRatio from df group by name order by FRRatio desc limit 1")
#hashtag = spark.sql("select name,FRRatio from df where FRRatio IN (select max(FRRatio) from df)")
hashtag = spark.sql("select name,max(FRRatio) as FRRatio from df group by name order by FRRatio desc limit 1")
#print(userSchema)
query = hashtag.writeStream.outputMode("complete").format("console").start()

query.awaitTermination(100)
query.stop()

