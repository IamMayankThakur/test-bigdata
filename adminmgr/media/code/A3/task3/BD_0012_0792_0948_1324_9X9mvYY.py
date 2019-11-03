# import findspark
# findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import re
#import requests

def aggregate_hashtags_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)
	
def splt(Z):
	s = re.split(r',',Z)
	for i in s:
		yield i

def computeContribs(urls):
	num_urls = len(urls)
	for url in urls:
        	yield (url,1)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,1)
ssc.checkpoint("/checkpoint")
v = int(sys.argv[1])
w=int(sys.argv[2])
StreamData=ssc.socketTextStream("localhost",9009)
hashtag1 = StreamData.filter(lambda z:z.split(';')[7]!="")
hashtag=hashtag1.map(lambda z:z.split(';')[7])
hashTag = hashtag.flatMap(lambda z: splt(z))
hashtag2 = hashTag.map( lambda x: (x,1))
#hashtag1=hashtag.map(lambda z:(z,1))
windowedHashtagCount = hashtag2.reduceByKeyAndWindow(lambda a, b: a + b, lambda a, b: a - b, v, w)
#windowedHashtagCount.pprint()
#totalcount=hashtag.updateStateByKey(aggregate_hashtags_count)
#totalcount.pprint()
#windowedWordCounts.pprint()
window = windowedHashtagCount.transform(lambda x : x.sortBy(lambda y : (-y[1],y[0])))
		
def sorting(time,rdd):
	value=rdd.collect()
	if(len(value)>4):
		print(value[0][0]+','+value[1][0]+','+value[2][0]+','+value[3][0]+','+value[4][0])
	#for val in value:
		#rdd.top(5, key=lambda x: x[5])
		#print(val[0][0],val[1][0],val[2][0],val[3][0],val[4][0])
window.foreachRDD(sorting)
#windowedHashtagCount.pprint()
ssc.start()
ssc.awaitTermination(60)
ssc.stop()
