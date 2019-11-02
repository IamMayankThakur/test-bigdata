# import findspark
# findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
#import requests

def aggregate_hashtags_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,2)
ssc.checkpoint("/checkpoint_BIGDATA")

StreamData=ssc.socketTextStream("localhost",9009)
#hashtag2=dataStream.filter(lambda w:(w.split(';')[7]!="0"))
hashtag=StreamData.flatMap(lambda z:z.split(';')[7].split(','))
#hashtag.pprint()
hashtag1=hashtag.map(lambda z:(z,1))
windowedHashtagCount = hashtag1.reduceByKeyAndWindow(lambda a, b: a + b, lambda a, b: a - b, 16, 8)

#totalcount=hashtag.updateStateByKey(aggregate_hashtags_count)
#totalcount.pprint()
#windowedWordCounts.pprint()
def sorting(time,rdd):
	value=sorted(rdd.collect(),key=lambda y:(-y[1]))
	for l,u in value:
		print("%s,%s"%(l,u))
windowedHashtagCount.foreachRDD(sorting)
ssc.start()
ssc.awaitTermination(25)
ssc.stop()

