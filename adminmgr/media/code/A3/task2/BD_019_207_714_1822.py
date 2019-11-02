from __future__ import print_function

import sys
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_obj = SparkSession.builder\
	.appName('StructuredSparkStreamingHadoop')\
	.getOrCreate()

schema1 = StructType([
	StructField('ID', IntegerType(), True),
	StructField('Lang', StringType(), True),
	StructField('Date', StringType(), True),
	StructField('Source', StringType(), True),
	StructField('Len', IntegerType(), True),
	StructField('Likes', IntegerType(), True),
	StructField('RTs', IntegerType(), True),
	StructField('Hashtags', StringType(), True),
	StructField('UserMentionNames', StringType(), True),
	StructField('UserMentionID', StringType(), True),
	StructField('Name', StringType(), True),
	StructField('Place', StringType(), True),
	StructField('Followers', IntegerType(), True),
	StructField('Friends', IntegerType(), True)
	])

count_dataframe = spark_obj.readStream\
.option('sep',';')\
.option('header','false')\
.schema(schema1)\
.csv('/stream')
count_dataframe.createOrReplaceTempView("table")
count_dataframe = spark_obj.sql("select Name as name,MAX(Followers/Friends) as FRRatio from table group by name order by FRRatio desc limit 5")
query_obj = count_dataframe.writeStream\
	.outputMode('complete').format('console')
query_runner = query_obj.start()
query_runner.awaitTermination(100)
query_runner.stop()
