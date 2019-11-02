import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from operator import add

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def tmp(tweet):
	temp = []
	if not tweet.split(";")[7]:
		temp.append("invalid")
	else:
		topics = tweet.split(";")[7].split(",")
		for i in topics:	
			temp.append(i)
	return temp

def process_rdd(time, rdd):
	temp = []
	for i in rdd.collect():
		temp.append(i[0])
	if temp:
		print(*temp, sep=",")


conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("~checkpoint_BIGDATA2")
dataStream=ssc.socketTextStream("localhost",9009)
newdata = dataStream.window(int(sys.argv[1]), 1)
tweet_count = newdata.flatMap(tmp).map(lambda x: (x,1))
tweet_count = tweet_count.filter(lambda a:a[0]!="invalid").reduceByKey(lambda a,b: a + b)
#tweet_count.pprint()
tweet_count = tweet_count.transform(lambda rdd : rdd.ctx.parallelize(rdd.takeOrdered(5,key=lambda a: (-a[1], a[0]))))
tweet_count.foreachRDD(process_rdd)
#tweet_count.pprint()

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
