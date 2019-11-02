import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import re
from operator import add
def process_rdd(time, rdd):
	#sql_context = get_sql_context_instance(rdd.context)
	#print("----------=========- %s -=========----------" % str(time))
	row_rdd = rdd.sortBy(lambda x:(-x[1],x[0])).take(5)
	#print(row_rd)
	hashtags=""
	if len(row_rdd)>1:
		for i in range(len(row_rdd)):
			if i==(len(row_rdd)-1):
				hashtags=hashtags+str(row_rdd[i][0])
			else: 
				hashtags=hashtags+str(row_rdd[i][0])+","
		print("%s"%(hashtags))
def tmp(x):
	 #(x.split(';')[7])
	parts=x.split(';')[7]		
	return parts

def hasht(x):
	#parts=x.split(',')
	parts=filter(None,x.split(','))
	for i in parts:
		return i
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
	ssc.checkpoint("~/checkpoint_BIGDATA")

	dataStream=ssc.socketTextStream("localhost",9009)
	# dataStream.pprint()
	tweet=dataStream.map(lambda x:tmp(x))
	#tweet.pprint()
	tweet=tweet.map(lambda x:hasht(x)).filter(lambda x:x!=None)
	#tweet.pprint()
	totalcount=tweet.countByValueAndWindow(window_size,1)
	#totalcount.pprint()

	#To Perform operation on each RDD
	totalcount.foreachRDD(process_rdd)

	ssc.start()
	ssc.awaitTermination(25)
	ssc.stop()
