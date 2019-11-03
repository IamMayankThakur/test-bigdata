import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests



	
def func(rdd):
	sorted_rdd = rdd.sortBy(lambda x: (-x[1],x[0])).filter(lambda y: y[0] !='')
	
	s_list=sorted_rdd.collect()
	if(s_list!=[]):
		print(s_list[0][0],s_list[1][0],s_list[2][0],s_list[3][0],s_list[4][0],sep=",")	
		
def func2(line):
	hashtag=line.split(";")[7]
	if(',' in hashtag):
		return hashtag.split(",")
	return [hashtag]


conf1=SparkConf()
conf1.setAppName("BigData")
sc1=SparkContext(conf=conf1)

sscp=StreamingContext(sc1,int(sys.argv[2]))
sscp.checkpoint("/checkpoint_BIGDATA")

dataStream1=sscp.socketTextStream("localhost",9009)


hashtags=dataStream1.window(int(sys.argv[1]),1).flatMap(func2).map(lambda h : (h,1)).reduceByKey(lambda x,y:int(x)+int(y))

hashtags.foreachRDD(func)


ssc.start()
ssc.awaitTermination(60)
ssc.stop()
