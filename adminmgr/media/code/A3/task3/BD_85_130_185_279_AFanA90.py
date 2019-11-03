import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

if __name__ == "__main__":
	#print(sys.argv[0],sys.argv[1],sys.argv[2])
	if len(sys.argv)!=3:
		sys.exit(-1)
def sortandprint(rdd):
	sorted_rdd1 = rdd.sortBy(lambda x: (x[1],x[0]))
	sorted_rdd=sorted_rdd1.filter(lambda y: y[0] !='')
	s_list=sorted_rdd.collect()
	print(s_list)
	#if(s_list!=[]):
	#	print(s_list[0][0],s_list[1][0],s_list[2][0],s_list[3][0],s_list[4][0],sep=",")	
		
    


ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("~/checkpointBIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
#pairs = dataStream.map(lambda word: (word.split(":")[7], 1))
words = dataStream.flatMap(lambda line: (line.split(";")[7].split(","),1))
totalcount1=words.reduceByKeyAndWindow(lambda x,y:x+y,sys.argv[1],1)
totalcount1.foreachRDD(sortandprint)

ssc.start()
ssc.awaitTermination(60)
ssc.stop()
