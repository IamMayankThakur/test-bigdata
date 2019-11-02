from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def printt(rdd):
	r=rdd.collect()
	#print(r)
	if(len(r)<5):
		lenn=len(r)
	else:
		lenn=5
	if(lenn==5):
		for i in range(5):
			if(i!=4):
				print(r[i][0],end=",")
			else:
				print(r[i][0])
	else:
		for i in range(lenn):
			if(i!=(lenn-1)):
				print(r[i][0],end=",")
			else:
				print(r[i][0])
if __name__ == "__main__":
	if len(sys.argv) != 3:
		sys.exit(-1)
	if int(sys.argv[1])<0 or int(sys.argv[2])<0 :
		sys.exit(-1)			
	conf=SparkConf()
	conf.setAppName("BigData")
	sc=SparkContext(conf=conf)
	ssc=StreamingContext(sc,int(sys.argv[2]))#according to be batch interval is this
	#ssc.checkpoint("/home/chaitra/Desktop/assignment3/checkpoint_BIGDATA")
	data=ssc.socketTextStream("localhost",9009)
	dataStream=data.window(int(sys.argv[1]), 1)#window,sliding
	tweet=dataStream.map(lambda w:(w.split(';')[7]))
	hashtags=tweet.flatMap(lambda w:(w.split(",")))
	hashtag=hashtags.map(lambda w:(w,1))
	countt=hashtag.reduceByKey(lambda x,y:x+y)
	countt=countt.filter(lambda w:w[0] != "")
	final = countt.transform(\
  	(lambda foo:foo\
   	.sortBy(lambda x:( x[0]))))
	final = final.transform(\
  	(lambda foo:foo\
   	.sortBy(lambda x:( -x[1]))))

	final.foreachRDD(printt)
	ssc.start()
	ssc.awaitTermination(25)
	ssc.stop()

