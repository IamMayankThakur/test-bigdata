import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,SparkSession
import sys
import requests
from pyspark.sql.types import *
from pyspark.sql.functions import *

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']



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
            StructField("Name",StringType(),True),
            StructField("Place",StringType(),True),
            StructField("Followers",IntegerType(),True),
            StructField("Friends",IntegerType(),True)
            ])

df = spark.readStream.schema(schema).format("csv").option("sep",";").load(input_path)

words = df.select(explode(split(df.Hashtags,",")).alias("Hashtags"))
wordCounts = words.groupBy("Hashtags").count().sort(col('count').desc()).select('Hashtags','count')

query = wordCounts \
    .limit(5)\
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()


query.awaitTermination(100)
query.stop()
