import findspark
findspark.init()

import time
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import explode, split, max as pymax
import sys
import requests

def get_schema():
        """get instance usage schema."""

        # Initialize columns for all string fields
        columns = ["tweetid", "lang", "date",
                   "source", "len", "likes", "rts", "hashtags",
                   "usermentionnames", "usermentionid",
                   "name",
                   "place"]

        columns_struct_fields = [StructField(field_name, StringType(), True)
                                 for field_name in columns]

        # Add columns for non-string fields
        columns_struct_fields.append(StructField("followers",
                                                 DoubleType(), True))
        columns_struct_fields.append(StructField("friends",
                                                 DoubleType(), True))
    
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


csvDF1 = csvDF.select(csvDF.name, (csvDF.followers/csvDF.friends).alias("ratio"))
csvDF1.createOrReplaceTempView("allratios")
csvDF3 = spark.sql("select name, sum(ratio) as FRRatio from allratios group by name")
csvDF3.createOrReplaceTempView("sumratios")
csvDF4 = spark.sql("select name, FRRatio from sumratios order by FRRatio desc limit 1")

query = csvDF4.writeStream.format("console").outputMode("complete").start()

query.awaitTermination(100)

ssc = spark.sparkContext
ssc.stop()

