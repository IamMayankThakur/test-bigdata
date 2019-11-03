# import findspark
# findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)
if(len(sys.argv) != 3):
	print("Usage: bin/submit-spark <filename> <window size> <batch duration>", file=sys.stderr)
	sys.exit(-1)

win_siz = int(sys.argv[1])
bat_dur = int(sys.argv[2])
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc, bat_dur)
ssc.checkpoint("~/Desktop/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)


def fun(x):
	h = x.split(";")[7]
	h = h.split(",")
	for i in h:
		if(i != ""):
			yield (i, 1)
hashtags=dataStream.flatMap(fun)
hashtags=hashtags.window(win_siz, 1)
count=hashtags.reduceByKeyAndWindow(lambda x,y:x+y, lambda x, y: x-y, win_siz, 1)

#count = count.transform(lambda rdd: rdd.sortBy(lambda a: (-a[1], a[0])))

def fun1(rdd):
	r = rdd.collect()
	if(len(r) <= 4):
		return
	r = sorted(r, key = lambda x: x[0])
	r = sorted(r, key = lambda x: -x[1])
	for i in range(5):
		if(i == 4):
			print(r[i][0])
			
		else:
			print(r[i][0], end = ",")
count.foreachRDD(fun1)
ssc.start()
ssc.awaitTermination(25)
ssc.stop()
