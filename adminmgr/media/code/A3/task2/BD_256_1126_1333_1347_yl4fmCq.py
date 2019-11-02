import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.types import StructType
import sys
import requests
from pyspark.sql.functions import desc
import pyspark.sql.functions as f
from pyspark.sql.functions import col


def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tweetid, no_of_tweets from hashtags")
			hashtag_counts_df.show()
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)

def tmp(x):
	return (x.split(';')[0],1)


spark = SparkSession \
    .builder \
    .appName("hashtagcount") \
    .getOrCreate()

userSchema=StructType().add("ID","string").add("Lang","string").add("Date","integer").add("Source","string").add("len","integer").add("Likes","integer").add("RTs","integer").add("Hashtags","string").add("UserMentionName","string").add("UserMentionID","string").add("name","string").add("Place","string").add("Followers","integer").add("Friends","integer")
df = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream")  # Equivalent to format("csv").load("/path/to/directory")

#hashtag = df.groupBy("Hashtags").count()
#hashtag = df.filter(df.Hashtags.isNotNull()).groupBy("Hashtags").count().withColumnRenamed("count","count").sort(desc("count")).limit(5)
"""
df1 = df.select("Name")
value = df.select("Name","Followers","Friends").withColumn("FRRatio",df["Followers"]/df["Friends"]).agg({"FRRatio":"max"})
ans = df1.withcolumn("FRRatio",value)

total = df.select("Name","Followers","Friends").agg({"count": "sum"}).collect().pop()['sum(count)']
result = df.withColumn('percent', (df['count']/total) * 100)
result.show()
"""
df1 = df.select("name","Followers","Friends").withColumn("FRRatio",(df["Followers"]/df["Friends"]))
df2 = df1.groupBy(df1['name']).agg(f.sum("FRRatio").alias('FRRatio'))
df3 = df2.sort(desc("FRRatio")).limit(1)
df4 = df3.select("name","FRRatio")

query = df4 \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination(100)
query.stop()



'''
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,2)
ssc.checkpoint("/home/sonu/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",1234)
# dataStream.pprint()
tweet=dataStream.map(tmp)
# OR
tweet=dataStream.map(lambda w:(w.split(';')[0],1))
count=tweet.reduceByKey(lambda x,y:x+y)
count.pprint()

#TO maintain state
# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()

#To Perform operation on each RDD
# totalcount.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(12)
ssc.stop()
'''
