import findspark
findspark.init()

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

dataStream=ssc.socketTextStream("localhost",9009)

def fun(x):
	x = x.split(",")
	for i in x:
		if i!="":
			yield (i, 1)

dataStream = dataStream.map(lambda x:x.split(';')[7])
hashtags=dataStream.flatMap(lambda x: fun(x))
hashtags = hashtags.window(win_siz,1)
count=hashtags.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,win_siz,1)
count = count.transform(lambda rdd: rdd.sortBy(lambda a: (-a[1], a[0])))
#count.pprint()
#count = count.transform(lambda rdd: sc.parallelize(rdd.take(3)))

def fun1(rdd):
	r = rdd.collect()

	if(len(r)>6):
		top_5 = r[0:5]
		for i in top_5[0:4]:
			print(i[0],end=",")
		print(top_5[4][0])



	
count.foreachRDD(fun1)
# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
