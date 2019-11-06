import findspark
findspark.init()
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

if "__name__" == "__main__":
      spark = SparkSession \
              .builder \
              .appName("BD_Assignment_1_2") \
              .getOrCreate()

      usrSchema = StructType().add("ID","string").add("Lang","string").add("Date","string").add("source","string").add("len",
                                                                                                                 "integer").add("likes","string").add("RTs","string").add("Hashtags",
                                                                                                                                                                          "string").add("UserMentionedName","string").add("UserMentionedID",
                                                                                                                                                                                                                          "string").add("Name","string").add("Place",
                                                                                                                                                                                                                                                             "string").add("Followers","string").add("Friends","string")
      df = spark \
           .readStream \
           .option("header","false") \
           .option("sep",";") \
           .scema(usrSchema) \
           .csv("hdfs://localhost:9000/stream")

      df.createOrReplaceTempView("table")

      t1 = spark.sql("SELECT name, Followers/Friends as FRRatio FROM table WHERE Friends  != 0 GROUP BY Name, FRRatio ORDER BY FRRatio DESC LIMIT 1")
      query = t1\
              .writeStream \
              .outputMode("complete") \
              .format("console") \
              .start()

      query.awaitTermination(100)
      query.stop()
      
