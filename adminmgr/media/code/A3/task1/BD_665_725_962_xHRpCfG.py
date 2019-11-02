from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_session_object = SparkSession.builder\
	.appName('StructuredSparkStreamingHadoop')\
	.getOrCreate()


schema = StructType().add('ID','integer')\
.add('Lang','string')\
.add('Date','string')\
.add('Source','string')\
.add('Len','integer')\
.add('Likes','integer')\
.add('RTs','integer')\
.add('Hashtags','string')\
.add('UserMentionNames','string')\
.add('UserMentionID','string')\
.add('Name','string')\
.add('Place','string')\
.add('Followers','integer')\
.add('Friends','integer')

df = spark_session_object.readStream\
.option('sep', ';')\
.option('header', 'false')\
.schema(schema)\
.csv('/stream')

df.createOrReplaceTempView("DataTable")
Hashtag_counts_dataframe=spark_session_object.sql("select Hashtags,count(Hashtags) as count from DataTable group by Hashtags order by count desc limit 5")

job = Hashtag_counts_dataframe.writeStream\
    .outputMode('complete').format('console')

query = job.start()
query.awaitTermination(110)
query.stop()
