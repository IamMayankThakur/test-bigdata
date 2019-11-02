from __future__ import print_function
from collections import defaultdict

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
window_size = int(sys.argv[1])
batch = int(sys.argv[2])

def tmp(x):
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
				break
				
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
ssc=StreamingContext(sc,batch)
ssc.checkpoint("checkpoint_BIGDATA")
dataStream=ssc.socketTextStream("localhost",9009)

tweets = dataStream.map(lambda w:(w.split(';')[7]))

tweets2 = tweets.map(tmp)

hashtags = tweets2.reduceByKeyAndWindow(lambda x, y: x + y, window_size, 1)

sortedhash = hashtags.transform(lambda w: w.sortBy(lambda x: (-x[1],x[0])))
sortedhash.foreachRDD(printrdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
