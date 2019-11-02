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
	l=[]
	for (word,count) in rdd.collect():
		if(word != ''):
			l.append(word) 	
			i += 1
		if i==5:
			break
	#print(l)
	print(l[0]+','+l[1]+','+l[2]+','+l[3]+','+l[4])
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
windowDuration = float(sys.argv[1])
batchSize = float(sys.argv[2])
ssc=StreamingContext(sc,batchSize) #batch interval
#ssc.checkpoint("/home/kishan/Downloads/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
windowStream = dataStream.window(windowDuration,2)
tweet=windowStream.map(lambda w: w.split(';')[7]).flatMap(lambda urls: parseNeighbors(urls))
#counts = tweet.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y) #window length
counts = tweet.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y) #window length
#counts = counts.sortBy(lambda x: x[1],ascending = False)

#.reduceByKey(lambda x,y: x+y)
#tweet.saveAsTextFile("hdfs://localhost:9000/spark/")
#tweet=tweet.map(lambda w:("Tweets in android",1))
#counts.pprint()
#totalcount=tweet.updateStateByKey(aggregate_tweets_count)
#totalcount.pprint()
sorted_ = counts.transform(
    lambda rdd: rdd.sortBy(lambda x: x[0]))
sorted_ = sorted_.transform(
    lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
sorted_.foreachRDD(printrdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()


