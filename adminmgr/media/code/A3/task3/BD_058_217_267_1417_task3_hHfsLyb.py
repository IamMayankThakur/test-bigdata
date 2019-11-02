from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def parseNeighbors(urls):
    parts = urls.split(',')
    for i in parts:
    	yield i
def printrdd(rdd):
	i=0
	r = rdd.collect()
	print(r[0][0],r[1][0],r[2][0],r[3][0],r[4][0],sep=",")
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
windowDuration = int(sys.argv[1])
batchSize = int(sys.argv[2])
ssc=StreamingContext(sc,batchSize) #batch interval
#ssc.checkpoint("/home/kishan/Downloads/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
windowStream = dataStream.window(windowDuration,1)
tweet=windowStream.map(lambda w: w.split(';')[7]).flatMap(lambda urls: parseNeighbors(urls))
#counts = tweet.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y) #window length
counts = tweet.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y) #window length
#counts = counts.sortBy(lambda x: x[1],ascending = False)

sorted_ = counts.transform(
    lambda rdd: rdd.sortBy(lambda x: x[0]))
sorted_ = sorted_.transform(
    lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

#sorted_.foreachRDD{ rdd => if(rdd.isEmpty){ print("") }else sorted_.foreachRDD(printrdd)}
sorted_.foreachRDD(printrdd)


ssc.start()
ssc.awaitTermination(60)
ssc.stop()


