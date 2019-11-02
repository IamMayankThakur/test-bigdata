
import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)


def takeAndPrint(rdd):
	taken = rdd.take(5)
	taken.sort()
	i = 0
	for record in taken[:5]:
		if(i != 4):
			print(record[0], end = ",")
		else:
			print(record[0], end = "\n")
		i+=1

def cleanData(x):
	hashtags = x.split(",")
	clean = []
	for hashtag in hashtags:
		if hashtag == " " or hashtag == "  " or hashtag == "":
		  	pass
		else:
		 	clean.append(hashtag)
	return clean

conf=SparkConf()
conf.setAppName("A2")
sc=SparkContext(conf=conf)

batch_size = sys.argv[2]
window_size = sys.argv[1]

ssc=StreamingContext(sc, float(batch_size))
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream = ssc.socketTextStream("localhost",9009)

datawindow = dataStream.window(float(window_size), 1)

tweet = datawindow.map(lambda w: w.split(';')[7])
tweet1 = tweet.flatMap(lambda w: cleanData(w))
tweet1 = tweet1.map(lambda x: (x, 1))


totalcount = tweet1.updateStateByKey(aggregate_tweets_count)

sorted_ = totalcount.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending = False))

sorted_.foreachRDD(takeAndPrint)


ssc.start()
ssc.awaitTermination(25)
ssc.stop()
