import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import re
from operator import add
def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
	#print("----------=========- %s -=========----------" % str(time))
	row_rdd = rdd.map(lambda w:(w[0],w[1])).take(5)
	hashtags=""
	for i in range(len(row_rdd)):
		if i==(len(row_rdd)-1):
			hashtags=hashtags+str(row_rdd[i][0])
		else:
			hashtags=hashtags+str(row_rdd[i][0])+","
	print("%s"%(hashtags))
def tmp(x):
	return (x.split(';')[7])
def hasht(x):
	parts=filter(None,x.split(','))
	for i in parts:
		return i,1
if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: pagerank <file> <Window Size> <Batch Duration>", file=sys.stderr)
		sys.exit(-1)
	window_size=int(sys.argv[1])	
	batch_durn=int(sys.argv[2])	
	conf=SparkConf()
	conf.setAppName("BigData")
	sc=SparkContext(conf=conf)

	ssc=StreamingContext(sc,batch_durn)
	ssc.checkpoint("home/chaitra/checkpoint_BIGDATA")

	dataStream=ssc.socketTextStream("localhost",9009)
	# dataStream.pprint()
	tweet=dataStream.map(tmp) 
	#tweet.pprint()
	# OR

	tweet=tweet.map(hasht).filter(lambda x:x!=None)
	#tweet.pprint()
	#count=tweet.groupByKey().count()
	#count.pprint()

	#TO maintain state
	#totalcount=tweet.updateStateByKey(aggregate_tweets_count)
	totalcount=tweet.reduceByKeyAndWindow(lambda x, y: x + y,lambda x,y:x-y,window_size).transform(lambda rdd: rdd.sortBy(lambda x: (-x[1],x[0])))
	#totalcount.pprint()

	#To Perform operation on each RDD
	totalcount.foreachRDD(process_rdd)

	ssc.start()
	ssc.awaitTermination(25)
	ssc.stop()
	
