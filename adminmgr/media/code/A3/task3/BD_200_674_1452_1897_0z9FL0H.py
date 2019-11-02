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
	#return sum(new_values)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
	num = 5
	taken = rdd.take(num + 1)
	l = []
	for record in taken[:num]:
		l.append(record[0])
	print(*l,sep = ",")
	
def tmp(x):

	return (x.split(';')[0],1)

def xyz(w):
	if(len(w.split(";")[7])==0):
		return ("Other",1)
	else:
		y = w.split(";")[7].split(",")
		for i in y:
			return(i,1)

'''
def pprint(senum=10):
	def takeAndPrint(time, rdd):
		taken = rdd.take(num + 1)
		for record in taken[:num]:
			print(record)
	self.foreachRDD(takeAndPrint)
'''



conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

#ssc=StreamingContext(sc,5)
ssc=StreamingContext(sc,float(sys.argv[2]))
#change this while debugging
ssc.checkpoint("/checkpoint_BIGDATA2")
dataStream=ssc.socketTextStream("localhost",9009)
#tweet=dataStream.map(xyz).filter(lambda x:x[0]!="Other").reduceByKeyAndWindow(lambda x, y: x + y, 30, 1)
tweet=dataStream.map(xyz).filter(lambda x:x[0]!="Other").reduceByKeyAndWindow(lambda x, y: x + y, int(sys.argv[1]), 1)
tweet = tweet.transform(lambda rdd : rdd.sortBy(lambda x:(-x[1],x[0])))
tweet.foreachRDD(process_rdd)	
#tweet.pprint()
ssc.start()
ssc.awaitTermination(25)
ssc.stop()
