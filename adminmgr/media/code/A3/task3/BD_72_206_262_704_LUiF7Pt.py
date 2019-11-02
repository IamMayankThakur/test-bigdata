from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import os
from pyspark.sql.types import *
from pyspark.sql.functions import *

# def sorting(x,y):
# 	if (x[1] == y[1]):
# 		if (x[0] > y[0]):
# 			return False
# 		if (x[0] < y[0]):
# 			return True
# 	else:
# 		if (x[1] > y[1]):
# 			return True
# 		else:
# 			return False

os.environ["PYSPARK_PYTHON"]='/usr/bin/python3'
os.environ["PYSPARK_DRIVER_PYTHON"]='/usr/bin/python3'
def output(rdd):
	mlist = []
	count = 0
	for i in rdd.take(6):
		if (count == 5):
			break
		if (i != [] and i[0]!=''):
			mlist.append(i)
			count+=1
	alist = [x[0] for x in mlist]
	if (alist != []):
		print(",".join(alist))
	
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
sc.setLogLevel("ERROR")
batch_interval = int(sys.argv[2])
window_size = int(sys.argv[1])
ssc=StreamingContext(sc,batch_interval)
ssc.checkpoint("/home/hadoop/checkpoint_BIGDATA424")

dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(lambda x: x.split(';')[7]).flatMap(lambda x: x.split(','))
tweet = tweet.countByValueAndWindow(window_size,batch_interval)
sortedtweets = tweet.transform(lambda rdd: rdd.sortBy(lambda x : (-x[1],x[0]),ascending=True))
sortedtweets.foreachRDD(lambda rdd: output(rdd))

ssc.start()
ssc.awaitTermination(100)
ssc.stop()
