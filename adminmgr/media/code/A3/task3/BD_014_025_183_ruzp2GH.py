import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def echo(time, rdd):
	counts = rdd.collect()
	if(rdd.count()!=0):
		print(counts[0][0],",",counts[1][0],",",counts[2][0],",",counts[3][0],",",counts[4][0],	sep="")
def tmp(x):
	splitter = x.split(';')
	for hashtags in splitter[7].split(','):
		if(hashtags != ''):
			yield (hashtags,1)

conf=SparkConf()
conf.setAppName("BigDataAssignment3")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
dataStream = ssc.socketTextStream("localhost",9009)
newStream = dataStream.window(int(sys.argv[1]),1).flatMap(tmp)
finalStream = newStream.reduceByKey(lambda x,y : x+y)
top_three_hash = finalStream.transform( lambda rdd: rdd.context.parallelize(rdd.takeOrdered(5, key = lambda x: (-x[1],x[0]))) )
top_three_hash.foreachRDD(echo)
ssc.start()
ssc.awaitTermination(25)
ssc.stop()
