from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spk_session_obj = SparkSession.builder\
	.appName('StructuredSpark')\
	.getOrCreate()

# lines_dataframe = spark_session_object.readStream\
# 	.format('socket')\
# 	.option('host', 'localhost').option('port', 9999)\
# 	.load()

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

words_dataframe = spark_session_object.readStream\
.option('sep', ';')\
.option('header', 'false')\
.schema(userSchema)\
.csv("hdfs://localhost:9000/stream")

words_dataframe.createOrReplaceTempView("tables")
friends_count_dataframe = spk_session_obj.sql("select Name, MAX(Followers/Friends) as FRRatio from tables group by Name order by FRRatio desc limit 1")
query_obj = friends_count_dataframe.writeStream\
	.outputMode('complete').format('console')

query_run = query_obj.start()
query_run.awaitTermination(100)
qwery_obj.stop()