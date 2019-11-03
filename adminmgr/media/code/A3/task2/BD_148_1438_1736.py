import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *

def finalquery(required):
    query = required \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    query.awaitTermination(100)
    query.stop()
spark = SparkSession\
    .builder\
    .appName("PopularUser")\
    .getOrCreate()
# Create DataFrame representing the stream of input lines from connection to host:port
data_schema =[
StructField("ID", StringType(), True),
StructField("Language", StringType(), True),
StructField("Date", StringType(), True),
StructField("Source", StringType(), True),
StructField("Length", IntegerType(), True),
StructField("Likes", IntegerType(), True),
StructField("Retweets", IntegerType(),True),
StructField("Hashtags", StringType(), True),
StructField("UserMentionNames", StringType(), True),
StructField("UserMentionID", StringType(), True),
StructField("Name", StringType(), True),
StructField("Place", StringType(), True),
StructField("Followers", IntegerType(), True),
StructField("Friends", IntegerType(), True)
]
finalschema = StructType(fields=data_schema)
v = spark \
    .readStream \
    .format("csv") \
    .option("sep", ";") \
    .schema(finalschema) \
    .load("hdfs://localhost:9000/stream")    
v.createOrReplaceTempView("twitter")
v=spark.sql("SELECT Name AS name, Followers/Friends AS FRRatio FROM twitter")
v=v.groupby("name","FRRatio").count()
v.createOrReplaceTempView("twitter")
top=spark.sql("SELECT name, FRRatio FROM twitter ORDER BY 2 DESC LIMIT 1")
finalquery(top)
