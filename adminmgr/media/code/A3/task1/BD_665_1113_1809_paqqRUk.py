from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split
from pyspark.sql.types import StructType
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split
from pyspark.sql.types import StructType


spark = SparkSession.builder.appName("task 1").getOrCreate()

# Read text from socket
'''socketDF = spark \
    .readStream \
    .format("csv") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
'''
#socketDF.isStreaming()    # Returns True for DataFrames that have streaming sources

#socketDF.printSchema()

# Read all the csv files written atomically in a directory
userSchema = StructType().add("ID", "string").add("Lang", "string").add("Date", "string").add("source", "string").add("Len", "string").add("Likes", "string").add("rts", "string").add("Hashtags", "string").add("usermn", "string").add("usermid", "string").add("name", "string").add("place", "string").add("followeres", "string").add("friends", "string")
csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/spark")  # Equivalent to format("csv").load("/path/to/directory")

#csvDF.show()
#print(csvDF['name'][0])
#sample2 = csvDF.rdd.map(lambda x: (x.name, x.age))
#print(sample2.collect())
#query=sample2.writeStream.start()
#hastgas=csvDF.select("hashtags")
words = csvDF.select(
   explode(
       split(csvDF.Hashtags, ",")
   ).alias("Hashtags")
)
#print(words)
wordCounts = words.groupBy("Hashtags").count()
sortedd=wordCounts.orderBy(["count"],ascending=False).limit(5)
query=sortedd.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(60)
query.stop()
#complete oly for groupBy ..others all append in outputmode

#for row in csvDF.rdd.collect():
#    print(row)
