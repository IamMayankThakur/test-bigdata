from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_session_object = SparkSession.builder.appName('structuredstreaming').getOrCreate()

#schema creation
schema = StructType([
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


#reading all csv files present in /stream 
lines = spark_session_object.readStream.option('sep', ';').option('header', 'false').schema(schema).csv('/stream')

lines.createOrReplaceTempView("task1b")
ratio_dataframe=spark_session_object.sql("select name,MAX(Followers/Friends) as FRRatio from task1b group by name order by FRRatio desc limit 1")
#execution of sql suery and output written as write stream
query = ratio_dataframe.writeStream.outputMode('complete').format('console')
query1b= query.start()
query1b.awaitTermination(60)
query1b.stop()
