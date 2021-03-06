import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def tmp(x):
	for i in x.split(';')[7].split(","):
		return i,1

def eachrdd(x):
	count = 0
	for i in x.collect():
		if i[0]:
			if count==4:
				print(i[0])
				break
			if count!=4:
				print(i[0], end=",")
				count+=1

batchDuration = int(sys.argv[2])
windowSize = int(sys.argv[1])

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
ssc=StreamingContext(sc,batchDuration)
ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)

tweet=dataStream.map(tmp)
count=tweet.reduceByKeyAndWindow(lambda x,y:x+y, lambda x,y:x-y, windowSize, 1)
a = count.transform(lambda rdd: rdd.sortBy(lambda x: x[0]))
b = a.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
b.foreachRDD(eachrdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()