from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_session_obj = SparkSession.builder\
	.appName('StructuredSpark')\
	.getOrCreate()

userSchema = StructType([
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

wordlin_dataframe = spark_session_obj.readStream\
.option('sep', ';')\
.option('header', 'false')\
.schema(userSchema)\
.csv("hdfs://localhost:9000/stream")

wordlin_dataframe.createOrReplaceTempView("tables")
common_hashtag_dataframe = spark_session_obj.sql("select Hashtags, count(Hashtags) as count from tables group by Hashtags order by count desc limit 5")
query_obj = common_hashtag_dataframe.writeStream\
	.outputMode('complete').format('console')

query_run = query_obj.start()
query_run.awaitTermination(100)
qwery_obj.stop()
