#import findspark
#findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests


def process_rdd(rdd):
	top = {1:(0,0),2:(0,0),3:(0,0)}	
	for value in rdd.collect():
		if(value[0] == ''):
			continue
		#print(value)
		if(value[1]>top[1][1]):
			top[3] = top[2]
			top[2] = top[1]
			top[1] = value
			continue
		if(value[1]>top[2][1]):
			top[3] = top[2]
			top[2] = value
			continue
		if(value[1]>top[3][1]):
			top[3] = value
	if(top[1][1]!=0):
		print(top[1][0],top[2][0],top[3][0])


WindowSize = int(sys.argv[1])
BatchDuration = int(sys.argv[2]) #pass window size and batch duration as command line arguments

#print(WindowSize, BatchDuration)

conf = SparkConf()
conf.setAppName("BigData")
sc = SparkContext(conf = conf)

ssc = StreamingContext(sc, BatchDuration) #passing batch duration
ssc.checkpoint("/checkpoint_BIGDATA")

socket_stream = ssc.socketTextStream("localhost", 9009) #stream the lines
lines = socket_stream.window(WindowSize)

cols = lines.flatMap(lambda line: [line.split(";")]) #split csv line into cols
#count=cols.reduceByKey(lambda x,y:x+y)
#cols.pprint()

hashtags = cols.flatMap(lambda col: col[7].split(",")) #split hashtag col into hashtags

hashtag_pairs = hashtags.map(lambda hashtag: (hashtag, 1)) #make (hashtag, 1) tuple

hashtagCounts = hashtag_pairs.reduceByKey(lambda x, y: x + y) #find counts

#hashtagCounts.pprint()

hashtagCounts.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(12)
ssc.stop()
