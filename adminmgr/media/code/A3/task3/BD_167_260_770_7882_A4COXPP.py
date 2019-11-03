#!/usr/bin/python3
import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def process_rdd1(time,rdd):
	i=0
	value=rdd.collect()
	value1=value[0:5]
	#print(value1)
	for K,V in value1:
		i=i+1
		print("%s"%(K), end="")
		if(i<5):
			print(',', end="")
		else:
			print()

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("~/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
# dataStream.pprint()
dataStream=dataStream.window(int(sys.argv[1]),1)
hashtags=dataStream.map(lambda w: w.split(';')[7])
#hashtags.pprint()

hashtags=hashtags.map(lambda x: (x,1))
hashtags=hashtags.map(lambda x: x[0].split(','))
hashtags=hashtags.flatMap(lambda x: x)
hashtags=hashtags.map(lambda x:(x,1))
hashtags=hashtags.filter(lambda x: x[0] is not '')
hashtag=hashtags.reduceByKey(lambda x,y: x+y)
#hashtag.pprint()
hashtag= hashtag.transform(lambda x: x.sortBy(lambda y:(-y[1],y[0])))
hashtag.foreachRDD(process_rdd1)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
