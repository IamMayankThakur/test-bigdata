from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_session_object = SparkSession.builder\
	.appName('StructuredSparkStreamingHadoop')\
	.getOrCreate()

# lines_dataframe = spark_session_object.readStream\
# 	.format('socket')\
# 	.option('host', 'localhost').option('port', 9999)\
# 	.load()

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
	StructField('name', StringType(), True),
	StructField('Place', StringType(), True),
	StructField('Followers', IntegerType(), True),
	StructField('Friends', IntegerType(), True)
	])

lines_dataframe = spark_session_object.readStream\
.option('sep', ';')\
.option('header', 'false')\
.schema(userSchema)\
.csv("hdfs://localhost:9000/stream")

#words_dataframe = lines_dataframe\
#	.select('*')

# words_dataframe = lines_dataframe.select(explode('Hashtags').alias('word'))

lines_dataframe.createOrReplaceTempView("tables")
word_counts_dataframe = spark_session_object.sql("select name, MAX(Followers/Friends) as FRRatio from tables group by name order by FRRatio desc limit 5")
query_object = word_counts_dataframe.writeStream\
	.outputMode('complete').format('console')

query_runner = query_object.start()
query_runner.awaitTermination(60)
qwery_object.stop()
