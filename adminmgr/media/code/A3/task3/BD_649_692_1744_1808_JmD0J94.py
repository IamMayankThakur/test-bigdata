import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from operator import add

def aggregate_tweets_count(values, sum):
	return sum(values) + (sum or 0)
	#return sum(values)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
	line = []
	for x in rdd.collect():
		line.append(x[0])
	print(*line, sep=",")

def tmp(x):
  return (x.split(';')[0],1)

def out(o):
    if(len(o.split(";")[7])!=0):
      y = o.split(";")[7].split(",")
      for i in y:
        return(i,1)
		
    else:
        return ("Other",1)
		


conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

#ssc=StreamingContext(sc,5)
ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("~/checkpoint_BIGDATA2")
dataStream=ssc.socketTextStream("localhost",9009)
#tweets=dataStream.map(out).filter(lambda x:x[0]!="Other").reduceByKeyAndWindow(lambda x, y: x + y, 30, 20)
tweets=dataStream.map(out).filter(lambda x:x[0]!="Other").reduceByKeyAndWindow(lambda x, y: x + y, int(sys.argv[1]), 10) #reduce the input based on the window size
#tweets = tweets.transform(lambda rdd : rdd.ctx.parallelize(rdd.top(5)))
tweets = tweets.transform(lambda rdd : rdd.ctx.parallelize(rdd.top(5,key=lambda x: x[1]))) #select top 5 hashtags
tweets.foreachRDD(process_rdd)
#tweets.pprint()
ssc.start()
ssc.awaitTermination(25)
ssc.stop()
