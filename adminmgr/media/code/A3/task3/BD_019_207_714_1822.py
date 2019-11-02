# import findspark
# findspark.init()
import re
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)
def flatt(url):
	#print(url)
	parts=re.split(r',',url)
	for u in parts:
		#print(u)
		yield (u,1)
if __name__=="__main__":
	if len(sys.argv)!=3:
		print("3 arguements required")
		sys.exit(-1)
	a=int(sys.argv[1])
	b=int(sys.argv[2])
	conf=SparkConf()
	conf.setAppName("BigData")
	sc=SparkContext(conf=conf)

	ssc=StreamingContext(sc,b)
	ssc.checkpoint("/home/manoj")

	dataStream=ssc.socketTextStream("localhost",9009)
	tweet2=dataStream.filter(lambda w:w.split(';')[7]!="")
	tweet=tweet2.map(lambda x:x.split(';')[7])
	#tweet1=tweet.map(lambda x:(x,1))
	job=tweet.flatMap(lambda x:flatt(x))
	windowedWordCounts = job.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, a, 1)
	#totalcount=tweet.updateStateByKey(aggregate_tweets_count)
	#totalcount.pprint()
	#windowedWordCounts.pprint()
	def gunf(time,rdd):		
		val=sorted(rdd.collect(),key=lambda x:(-x[1]))
		i=0
		if(len(val)>4):
			print(val[0][0]+","+val[1][0]+","+val[2][0]+","+val[3][0]+","+val[4][0])
		#print("%s,%s,%s"%(val[0][0],val[1][0],val[2][0],val[3][0],val[4][0]))
	windowedWordCounts.foreachRDD(gunf)
	ssc.start()
	ssc.awaitTermination(30)
	ssc.stop()
