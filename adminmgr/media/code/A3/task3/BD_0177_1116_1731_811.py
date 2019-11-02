# import findspark
# findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import re

"""
def split_func(u):
	p=re.split(r',',url)
	for k in p:
		yield(k,1)
"""


if len(sys.argv)!=3:
	sys.exit(-1)

p=int(sys.argv[1])
q=int(sys.argv[2])
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,1)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
t_1=dataStream.filter(lambda w:(w.split(';')[7]!="0" and w.split(';')[7]!=""))
t_2=t_1.map(lambda x:x.split(';')[7])
#tweet.pprint()
t_3=t_2.map(lambda x:(x,1))
#tweet1.pprint()

per_window_word_count= t_3.reduceByKeyAndWindow(lambda i,j:i+j, lambda i,j:i-j,p,q)



def print_common_hashtags(time,rdd):
	v=sorted(rdd.collect(),key=lambda z:(-z[1],z[0]))
	if(len(v)):
		#print(v)		
		print("%s,%s,%s,%s,%s"%(v[0][0],v[1][0],v[2][0],v[3][0],v[4][0]))
	
	
per_window_word_count.foreachRDD(print_common_hashtags)
ssc.start()
ssc.awaitTermination(30)
ssc.stop()
