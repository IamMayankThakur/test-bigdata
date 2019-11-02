# import findspark
# findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def process_rdd(time, rdd):
	final = rdd.sortBy(lambda x:x[0])
	final1= rdd.sortBy(lambda x:x[1],ascending= False).collect()[:5]
	if(final1==[]):
		return
	else:
		for i in range(0,4):
			print(final1[i][0],end=',')
		print(final1[4][0])
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

a=int(sys.argv[1])
b=int(sys.argv[2])

ssc=StreamingContext(sc,1)
#ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
count=dataStream.window(a,b)
tweet=count.map(lambda w:w.split(';')[7])
tweet1=tweet.flatMap(lambda w:(w.split(',')))
tweet2=tweet1.map(lambda w:(w,0) if w is '' else (w,1))
tweet3 = tweet2.reduceByKey(lambda x,y:x+y)
tweet3.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
