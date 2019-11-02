import findspark
findspark.init()

import time
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import explode, split
import sys
import requests




def get_schema():
        """get instance usage schema."""

        # Initialize columns for all string fields
        columns = ["tweetid", "lang", "date",
                   "source", "len", "likes", "rts", "hashtags",
                   "usermentionnames", "usermentionid",
                   "name",
                   "place", "followers",
                   "friends"]

        columns_struct_fields = [StructField(field_name, StringType(), True)
                                 for field_name in columns]

        schema = StructType(columns_struct_fields)

        return schema

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], hashtags=w[7]))
			print(row_rdd)
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("allhashtags")
			hashtag_counts_df = sql_context.sql("select tweetid, hashtags from allhashtags")
			hashtag_counts_df.show()
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)

def tmp(x):
	return (x.split(';')[0],1)

spark = SparkSession \
    .builder \
    .appName("BigData") \
    .getOrCreate()

userSchema = get_schema()

csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("/stream")


csvDF1 = csvDF.select(explode(split(csvDF.hashtags, ",")).alias("Hashtags"))
csvDF1.createOrReplaceTempView("split")
csvSqlDF = spark.sql("select Hashtags, count(*) as count from split group by Hashtags order by count desc limit 5")

query = csvSqlDF.writeStream.format("console").outputMode("complete").start()
query.awaitTermination(100)

ssc = spark.sparkContext
ssc.stop()

