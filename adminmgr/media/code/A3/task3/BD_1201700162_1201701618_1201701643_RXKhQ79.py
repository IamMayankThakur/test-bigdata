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
	row_rdd = rdd.sortBy(lambda x:-x[1])#.take(5)
	row_rdd=row_rdd.collect()
	if len(row_rdd)>0:
		hashtags=str(row_rdd[0][0])+","+str(row_rdd[1][0])+","+str(row_rdd[2][0])+","+str(row_rdd[3][0])+","+str(row_rdd[4][0])+""
		print("%s"%(hashtags))
	"""hashtags=""
	for i in range(len(row_rdd)):
		if i==(len(row_rdd)-1):
			hashtags=hashtags+str(row_rdd[i][0])
		else:
			hashtags=hashtags+str(row_rdd[i][0])+","
	print("%s"%(hashtags))"""
def tmp(x):
	return (x.split(';')[7])
def hasht(x):
	parts=x.split(',')
	for i in parts:
		if len(i)>1:
			yield (i,1)
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

	tweet=tweet.flatMap(lambda x:hasht(x))#.filter(lambda x:x!=None)

	totalcount=tweet.window(window_size,1).reduceByKey(add)
	#totalcount.pprint()

	#To Perform operation on each RDD
	totalcount.foreachRDD(process_rdd)

	ssc.start()
	ssc.awaitTermination(60)
	ssc.stop()

