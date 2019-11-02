from __future__ import print_function

import findspark
findspark.init()

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_session_object = SparkSession.builder\
	.appName('BD_A3_T1_1')\
	.getOrCreate()

my_schema = StructType([
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

lines_dataframe = spark_session_object.readStream\
	.option('sep', ';')\
	.option('header', 'false')\
	.schema(my_schema)\
	.csv('/stream')

lines_dataframe.createOrReplaceTempView("tweets")

word_counts_dataframe = spark_session_object.sql("select Hashtags as Hashtags, count(Hashtags) as count from tweets group by Hashtags order by count desc limit 5")

query_object = word_counts_dataframe.writeStream\
	.outputMode('complete').format('console')

query_object.start().awaitTermination(100)
spark_session_object.stop()
