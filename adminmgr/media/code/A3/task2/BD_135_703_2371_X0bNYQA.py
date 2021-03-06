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
		
    


conf1=SparkConf()
conf1.setAppName("BigData")
sc1=SparkContext(conf=conf1)

sscp=StreamingContext(sc1,int(sys.argv[2]))
sscp.checkpoint("/checkpoint_BIGDATA")

dataStream1=sscp.socketTextStream("localhost",9009)
hashtag1=dataStream1.window(int(sys.argv[1]),1)

if(',' in hashtag1.select(lambda w: w.split(";")[7])):
	hashtag2=hashtag1.select(lambda w: w.split(";")[7])
	hashtag3=hashtag2.flatmap(lambda p:p.split(","))
else:
	hashtag3=hashtag1.flatmap(lambda w: w.split(";")[7])
hashtags = hashtag3.map(lambda x: (x,1)).reduceByKey(lambda x,y:int(x)+int(y))
#hashtags=hashtag4.reduceByKey(add)
#hashtags=hashtag4.reduceByKey(lambda x,y:int(x)+int(y))
hashtags.foreachRDD(func)


ssc.start()
ssc.awaitTermination(60)
ssc.stop()
