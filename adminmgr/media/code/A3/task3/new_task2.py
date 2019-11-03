from __future__ import print_function
import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


'''def most_hashtags(new_state,):
    final_hash = defaultdict(lambda: 0)
    hashtags = tweet.split(',')
    for i in hashtags:
        final_hash[i] +=1
    sorted_hash = sorted(final_hash,key = d.get, reverse =  True)[:3]
'''
# input

def line_split(x):
	return x.split(";")[7].split(",")



'''def tmp(x):
	for i in x.split(','):
		return(i,1)

def printrdd(rdd):
	line = 0
	for i in rdd.collect():
		if(i[0]):
			if(line != 4):
				#print(5)
				print(i[0],end = ",")
				line += 1
			else:
				print(i[0])	
				break'''

def printrdd2(rdd):
	temp_rdd = rdd.sortBy(lambda a: (-a[1],a[0])).filter(lambda x : x[0] != '')
    	temp_rdd_2 = temp_rdd.collect()
    	if(temp_rdd_2 != []):
		for i in range(5):
			if(i != 4):
				print(temp_rdd_2[i][0],end=",")
			else:
				print(temp_rdd_2[i][0])

window_size = int(sys.argv[1])
batch = int(sys.argv[2])


conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,batch)
ssc.checkpoint("~/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)

tweets = dataStream.window(window_size,1)
flat_tweets = tweets.flatMap(line_split).map(lambda w:(w, 1))
reduced_tweets = flat_tweets.reduceByKey(lambda x,y:int(x)+int(y))

reduced_tweets.foreachRDD(printrdd2)

ssc.start()
ssc.awaitTermination(60)
ssc.stop()
