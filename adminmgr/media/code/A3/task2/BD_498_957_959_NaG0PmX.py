import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,SparkSession
import sys
import requests
from pyspark.sql.types import *
from pyspark.sql.functions import *
import math



def cal(x,y):
    a=int(x)
    b=int(y)
    if a==0 or b==0:
        return "0:0"
    z=math.gcd(a,b)
    return (str(a/z)+":"+str(b/z))

def add_col(in_1):
    return in_1.withColumn('ratio',func(in_1['Followers'] , in_1['Friends']))


spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

input_path = "/stream"

#ID, Lang, Date, Source, len, Likes, RTs, Hashtags, UserMentionNames, UserMentionID, Name, Place, Followers,Friends
schema = StructType([ StructField("ID",StringType(), True),
            StructField("Lang",StringType(),True),
            StructField("Date",TimestampType(),True),
            StructField("Source",StringType(),True),
            StructField("len",IntegerType(),True),
            StructField("Likes",IntegerType(),True),
            StructField("RT's",StringType(),True),
            StructField("Hashtags", StringType(),True),
            StructField("UserMentionNames",StringType(),True),
            StructField("UserMentionID",StringType(), True),
            StructField("name",StringType(),True),
            StructField("Place",StringType(),True),
            StructField("Followers",IntegerType(),True),
            StructField("Friends",IntegerType(),True)
            ])

df = spark.readStream.schema(schema).format("csv").option("sep", ";").load(input_path)

words = df.select('name', (df.Followers / df.Friends).alias("FRRatio") )
wordCounts = words.groupBy("name","FRRatio").count().orderBy(desc("FRRatio"))
wordCounts = wordCounts.select("name","FRRatio")


query = wordCounts \
    .limit(1) \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()


query.awaitTermination(100)
query.stop()
