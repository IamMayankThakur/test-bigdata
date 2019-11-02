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

ssc=StreamingContext(sc,bat_dur)
ssc.checkpoint("~/Desktop/checkpoint_BIGDATA")
def tmp(x):
	x.pprint()
	return (x.split(';')[0],1)
dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(tmp)
tweet=tweet.window(win_siz, 1)

def fun(x):
	h = x.split(";")[7]
	h = h.split(",")
	for i in h:
		yield (i, 1)

hashtags=dataStream.flatMap(fun)
hashtags=hashtags.filter(lambda x:False if x[0] == "" else True)
count=hashtags.reduceByKey(lambda x,y:x+y)

count = count.transform(lambda rdd: rdd.sortBy(lambda a: (-a[1], a[0])))
#count = count.transform(lambda rdd: sc.parallelize(rdd.take(3)))

def fun1(rdd):
	r = rdd.collect()
	for i in range(5):
		if(i == 4):
			print(r[i][0])
			
		else:
			print(r[i][0], end = ",")
count.foreachRDD(fun1)

	

# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
