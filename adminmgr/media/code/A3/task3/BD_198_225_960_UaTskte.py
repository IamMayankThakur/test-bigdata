import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
#from pyspark.sql import Row,SQLContext
import sys
import requests
import re
from operator import add

def process_rdd(time, rdd):
#	print("----------=========- %s -=========----------" % str(time))
	row_rdd = rdd.map(lambda w:(w[0],w[1]))
	maximum = row_rdd.take(5)
	hashh=""
	i=0	
	while i<len(maximum):
		
		if i==(len(maximum)-1):
			hashh=hashh+str(maximum[i][0])
		else:
			hashh=hashh+str(maximum[i][0])+","
		i=i+1
	if hashh!="":
		print("%s"%(hashh))





wind_size=int(sys.argv[1])	
batch_duration=int(sys.argv[2])	
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,batch_duration)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)

tweet=dataStream.map(lambda w:(w.split(';')[7]))

hashtag=tweet.flatMap(lambda w:(w.split(',')))
hasht=hashtag.map(lambda w:(w,1))
counts=hasht.filter(lambda x:x[0]!='')

totalcount=counts.window(wind_size, 1).reduceByKey(lambda a,b: a+b)
tc=totalcount.transform(lambda rdd: rdd.sortBy(lambda y: (-y[1],y[0])))
#print(totalcount)
tc.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(60)
ssc.stop()
