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
	parts=x.split(';')[7]
	if (',' in parts):
		part=parts.split(',')
		for i in part:
			return (i,1)
	else: 
		return None

window_size=int(sys.argv[1])	
batch_durn=int(sys.argv[2])	
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
ssc=StreamingContext(sc,batch_durn)
ssc.checkpoint("~/checkpoint_BIGDATA")
dataStream=ssc.socketTextStream("localhost",9009)
	# dataStream.pprint()
#tweet=dataStream.map(lambda x:tmp(x)).filter(lambda x:x!=None) 

#tweet=tweet.flatMap(lambda x:hasht(x)).reduceByKey(lambda x,y:x+y)

totalcount=dataStream.window(window_size,1).map(lambda x:tmp(x)).filter(lambda x:x!=None).reduceByKey(add)
	#totalcount.pprint()

	#To Perform operation on each RDD
totalcount.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
