import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


def tmp(x):
	y = (x.split(';')[7]).split(',')
	return (y)

def forf(x):
	for i in x:
		yield (i,1)

def topprint(time,rdd):
	res1=rdd.take(5)
	count=0
	for i in res1:
		if(count==4):
			print("%s" % i)
		else:
			print("%s" % i,end=',')
		count = count +1



conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,1)
ssc.checkpoint("/checkpoint_BIGDATA")




#Selecting a datastream and then reducing by window:
#outpu2
dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(tmp)
septweet=tweet.flatMap(forf)
#septweet.pprint()
count=septweet.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,int(sys.argv[1]),int(sys.argv[2]))
sortcount = count.transform(lambda rdd :rdd.sortBy(lambda a:a[0],ascending=True))
sortcount = count.transform(lambda rdd :rdd.sortBy(lambda a:a[1],ascending=False))
tweet1=sortcount.filter(lambda w:w[0] is not '')
#tweet1.pprint()
res = tweet1.map(lambda a : a[0])
res.foreachRDD(topprint)

'''

#Try in outpu1
inputStream=ssc.socketTextStream("localhost",9009)
dataStream = inputStream.window(int(sys.argv[1]),int(sys.argv[2]))
tweet=dataStream.map(tmp)
septweet=tweet.flatMap(forf)
count=septweet.reduceByKey(lambda x,y:x+y)
sortcount = count.transform(lambda rdd :rdd.sortBy(lambda a:a[0],ascending=True))
sortcount1 = sortcount.transform(lambda rdd :rdd.sortBy(lambda a:a[1],ascending=False))
tweet1=sortcount1.filter(lambda w:w[0] is not '')
#tweet1.pprint()
res = tweet1.map(lambda a : a[0])
res.foreachRDD(topprint)

'''
	


ssc.start()
ssc.awaitTermination(25)
ssc.stop()
