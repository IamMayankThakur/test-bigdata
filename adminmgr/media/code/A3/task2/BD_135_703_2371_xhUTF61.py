import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


	
def func(rdd):
	sorted_rdd1 = rdd.sortBy(lambda x: (x[1],x[0]))
	sorted_rdd=sorted_rdd1.filter(lambda y: y[0] !='')
	s_list=sorted_rdd.collect()
	if(s_list!=[]):
		print(s_list[0][0],s_list[1][0],s_list[2][0],s_list[3][0],s_list[4][0],sep=",")	
		
    


conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
hashtag1=dataStream.window(int(sys.argv[1]),1)

if(',' in hashtag1.select(lambda w: w.split(";")[7])):
	hashtag2=hashtag1.select(lambda w: w.split(";")[7])
	hashtag3=hashtag2.flatmap(lambda p:p.split(","))
else:
	hashtag3=hashtag1.flatmap(lambda w: w.split(";")[7])
hashtag4 = hashtag3.map(lambda x: (x,1))
#hashtags=hashtag4.reduceByKey(add)
hashtags=hashtag4.updateStateByKey(lambda x,y:int(x)+int(y))
hashtags.foreachRDD(func)


ssc.start()
ssc.awaitTermination(25)
ssc.stop()
