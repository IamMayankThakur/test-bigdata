# import findspark
# findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
#import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,2)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
#tweet2=dataStream.filter(lambda w:(w.split(';')[7]!="0"))
tweet=dataStream.flatmap(lambda x:x.split(';')[7].split(','))
#tweet.pprint()
tweet1=tweet.map(lambda x:(x,1))
#tweet1.pprint()
#wordcount=tweet1.reduceByKey(lambda x,y:x+y)
#wordcount.pprint()
windowedWordCounts = tweet1.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 16, 8)

#totalcount=tweet.updateStateByKey(aggregate_tweets_count)
#totalcount.pprint()
#windowedWordCounts.pprint()
def gunf(time,rdd):
	val=sorted(rdd.collect(),key=lambda x:(-x[1]))
	for k,v in val:
		print("%s,%s"%(k,v))
windowedWordCounts.foreachRDD(gunf)
ssc.start()
ssc.awaitTermination(25)
ssc.stop()

