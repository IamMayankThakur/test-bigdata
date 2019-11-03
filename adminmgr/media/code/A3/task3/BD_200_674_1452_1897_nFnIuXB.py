import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)
	#return sum(new_values)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(rdd):
	sorted_rdd = rdd.sortBy(lambda a:(-a[1],a[0])).filter(lambda x:x[0]!='')
	sorted_list = sorted_rdd.collect()
	if(sorted_list!=[]):
		print(sorted_list[0][0],sorted_list[1][0],sorted_list[2][0],sorted_list[3][0],sorted_list[4][0],sep=",")
	
def tmp(x):

	return (x.split(';')[0],1)

def xyz(w):
	hashtags = w.split(";")[7]
	if(',' in hashtags):
		return hashtags.split(",")
	return [hashtags] 


conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

#ssc=StreamingContext(sc,5)
ssc=StreamingContext(sc,int(sys.argv[2]))
#change this while debugging
ssc.checkpoint("~/checkpoint_BIGDATA")
dataStream=ssc.socketTextStream("localhost",9009)
tweet = dataStream.window(int(sys.argv[1]),1).flatMap(xyz).map(lambda x:(x,1)).reduceByKey(lambda m,n:int(m)+int(n))
tweet.foreachRDD(process_rdd)	
ssc.start()
ssc.awaitTermination(60)
ssc.stop()
