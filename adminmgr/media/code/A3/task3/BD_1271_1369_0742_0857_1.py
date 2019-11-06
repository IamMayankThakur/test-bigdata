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

if "__name__" ==  "__main__":

      spark = SparkSession \
                .builder \
                .appName("StructuredNetworkWordCount") \
                .getOrCreate()
      usrSchema = StructType().add("ID","string").add("Lang","string").add("Date","string").add("source","string").add("len",
                                                                                                                 "integer").add("likes","string").add("RTs","string").add("Hashtags",
                                                                                                                                                                          "string").add("UserMentionedName","string").add("UserMentionedID",
                                                                                                                                                                                                                          "string").add("Name","string").add("Place",
                                                                                                                                                                                                                                                             "string").add("Followers","string").add("Friends","string")


      df = spark \
          .readStream \
          .option("sep", ";") \
          .schema(usrSchema) \
          .csv("hdfs://localhost:9009/stream")

      #df.createOrReplaceTempView("update")

      t1 = df.select(explode(split(df.Hashtags,",")).alias("hashtag"))

      t2 = t1.groupBy("hashtag").count()
      t3 = t2.top.orderBy("count",ascending=False)
      t4 = t3.limit(5)

      query = t4 \
          .writeStream \
          .outputMode("complete") \
          .format("console") \
          .start()

      
      query.awaitTermination(100)
      query.stop()
