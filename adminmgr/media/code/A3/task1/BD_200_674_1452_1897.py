import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from operator import add

import pyspark
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split,max
sc = SparkContext("local")
spark = SparkSession(sc)
"""spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()"""
inputpath = "/stream"   
# Read all the csv files written atomically in a directory
userSchema = """StructType().add("ID","string").add("Lang","string").add("Date","string").add("Source","string").add("len","string").add("Likes","string").add("RTs","string").add("Hashtags","string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","string").add("Friends","string")
"""
userSchema = StructType([StructField("ID",StringType(),True),
			StructField("Lang",StringType(),True),
			StructField("Date",StringType(),True),
			StructField("Source",StringType(),True),
			StructField("len",StringType(),True),
			StructField("Likes",StringType(),True),
			StructField("RTs",StringType(),True),
			StructField("Hashtags",StringType(),True),
			StructField("UserMentionNames",StringType(),True),
			StructField("UserMentionID",StringType(),True),
			StructField("name",StringType(),True),
			StructField("Place",StringType(),True),
			StructField("Followers",IntegerType(),True),
			StructField("Friends",IntegerType(),True)])
csvDF =( spark \
    .readStream \
    .schema(userSchema) \
    .option("delimiter", ";") \
    .option("maxFilesPerTrigger",1)\
    .csv(inputpath))
query1 = csvDF.select(explode(split("Hashtags",",")).alias("Hashtags")).groupBy("Hashtags").count().orderBy('count',ascending= False)

query1.writeStream\
	.outputMode("complete")\
	.format("console")\
	.option("numrows" ,5)\
	.queryName("counts")\
	.start()\
	.awaitTermination(60)
query1.stop()
