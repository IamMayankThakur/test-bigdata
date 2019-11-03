# import findspark
# findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
#import requests

def aggregate_hashtags_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def computeContribs(urls):
	num_urls = len(urls)
	for url in urls:
        	yield (url,1)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,1)
ssc.checkpoint("/checkpoint")

StreamData=ssc.socketTextStream("localhost",9009)
hashtag1 = StreamData.filter(lambda z:z.split(';')[7]!="")
hashtag=hashtag1.flatMap(lambda z:z.split(';')[7].split(','),1)
hashtag2 = hashtag.map( lambda x: computeContributs(x))
#hashtag1=hashtag.map(lambda z:(z,1))
windowedHashtagCount = hashtag2.reduceByKeyAndWindow(lambda a, b: a + b, lambda a, b: a - b, 10, 1)

#totalcount=hashtag.updateStateByKey(aggregate_hashtags_count)
#totalcount.pprint()
#windowedWordCounts.pprint()
def sorting(time,rdd):
	value=sorted(rdd.collect(),key=lambda y:(-y[1],y[0]))
	ct=0
	val=''
	for l,u in value:
		#rdd.top(5, key=lambda x: x[5])
		if ct ==5:
			val+=l
		else:
			val+=l+','
		ct+=1	
		#print("%s,%s,%s,%s,%s" %(l[0][0],l[1][0],l[2][0],l[3][0],l[4][0]))
	print(val)
windowedHashtagCount.foreachRDD(sorting)
#windowedHashtagCount.pprint()
ssc.start()
ssc.awaitTerminationOrTimeout(20)
ssc.stop()
