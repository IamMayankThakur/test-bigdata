# -*- coding: utf-8 -*-
"""BigData3_1.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1f7eHV3IWrMYAZI-yGNJd_Pn_0x2Fylxw
"""

from pyspark.sql.types import TimestampType, StringType, IntegerType, StructType, StructField
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
#Importing the data types needed to define the schema

spark = SparkSession \
    .builder \
    .appName("Task1Assignment3") \
    .getOrCreate()
  

# Path to the file
inputPath = "hdfs://localhost:9000/stream/" #What to initialise this to? 
 
#ID, Lang, Date, Source, len, Likes, RTs, Hashtags, UserMentionNames, UserMentionID, Name, Place, Followers, Friends
#Setting appropriate data type
#Link used for reference is https://hackersandslackers.com/structured-streaming-in-pyspark/ 
schema = StructType([ StructField("ID", StringType()),
                      StructField("Lang", StringType()),
                      StructField("Date", TimestampType()),
                      StructField("Source", StringType()),
                      StructField("len", IntegerType()),
                      StructField("Likes", IntegerType()),
                      StructField("RTs", IntegerType()),
                      StructField("Hashtags", StringType()),
                      StructField("UserMentionNames", StringType()),
                      StructField("UserMentionID", StringType()),
                      StructField("Name", StringType()),
                      StructField("Place", StringType()),
                      StructField("Followers", IntegerType()),
                      StructField("Friends", IntegerType())])

'''inputDF = (
 spark \
    .readStream \
    .option("sep", ";") \
    .option('maxFilesPerTrigger', 1)\
    .schema(schema) \
    .csv(inputPath)
)'''

DataFrame1 = (
  spark \
    .readStream \
    .option("sep", ";") \
    .option('maxFilesPerTrigger', 1) \
    .schema(schema) \
    .csv(inputPath)    
    
)

#Using Spark Documentation
#https://spark.apache.org/docs/2.3.3/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets
#spark.conf.set("spark.sql.shuffle.partitions", "2")
#Caching in memory 
#Milan said not needed
'''
words = DataFrame1.select(
   explode(
       split(DataFrame1.Hashtags, ",")
   ).alias("word")
)
'''
DataFrame1.createOrReplaceTempView("HashtagView")
#words.createOrReplaceTempView("HashtagView")
#SELECT column_name(s)
#FROM table_name
#WHERE condition
#GROUP BY column_name(s)
#ORDER BY column_name(s);
#w3 schools SQL


#HashCounter = spark.sql("SELECT Hashtags as Hashtags, count(*) as count FROM HashtagView GROUP BY Hashtags ORDER BY count DESC LIMIT 5")
HashCounter = spark.sql("SELECT Hashtags as Hashtags, count(Hashtags) as count FROM HashtagView GROUP BY Hashtags ORDER BY count DESC LIMIT 5")


query = ( HashCounter \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
)

query.awaitTermination(100)
query.stop()
spark.stop()

