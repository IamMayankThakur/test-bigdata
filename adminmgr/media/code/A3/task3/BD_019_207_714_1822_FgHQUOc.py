import findspark
findspark.init()
import re
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)
def flatt(url):
	parts=re.split(r',',url)
	for u in parts:
		yield (str(u),1)
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
	ssc.checkpoint("/some")
	dataStream=ssc.socketTextStream("localhost",9009)
	tweet2=dataStream.filter(lambda w:w.split(';')[7]!="")
	tweet=tweet2.map(lambda x:x.split(';')[7])
	job=tweet.flatMap(lambda x:flatt(x))
	windowedWordCounts = job.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, a, 1)
	def gunf(time,rdd):		
		val=sorted(rdd.collect(),key=lambda x:(-x[1],x[0]))
		i=0
		if(len(val)>4):
			print(val[0][0]+","+val[1][0]+","+val[2][0]+","+val[3][0]+","+val[4][0])
	windowedWordCounts.foreachRDD(gunf)
	ssc.start()
	ssc.awaitTermination(30)
	ssc.stop()
