from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_session_object = SparkSession.builder.appName('structuredstreaming').getOrCreate()

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
StructField('Name', StringType(), True),
StructField('Place', StringType(), True),
StructField('Followers', IntegerType(), True),
StructField('Friends', IntegerType(), True)
])

lines = spark_session_object.readStream.option('sep', ';').option('header', 'false').schema(schema).csv('/stream')

lines.createOrReplaceTempView("task1a")
hash_dataframe=spark_session_object.sql("select Hashtags,count(Hashtags) as count from task1a group by Hashtags order by count desc limit 5")
query= hash_dataframe.writeStream.outputMode('complete').format('console')
query1a= query.start()
query1a.awaitTermination(60)
query1a.stop()

