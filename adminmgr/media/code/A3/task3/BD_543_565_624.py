#import findspark
#findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
#ssc.checkpoint("/home/ccbdprojectsav")

dataStream=ssc.socketTextStream("localhost",9009)
#tweet=dataStream.filter(lambda w: w.split(';')[7].strip())
#tweet.pprint()
tweet=dataStream.map(lambda w:w.split(';')[7].strip())
#tweet=tweet.flatMap(lambda w:list(map(lambda x:x,w.split(','))))
tweet=tweet.flatMap(lambda x:x.split(','))
tweet=tweet.filter(lambda x:x!='')
tweet=tweet.map(lambda x:(x,1))
#tweet.pprint()
#tweet.pprint()
#tweet1=tweet.countByValue()
#tweet=tweet.map(lambda w:w[0])
#hi=tweet.map(lambda
#tweet.pprint()
commonhashtags = tweet.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,int(sys.argv[1]), 1)
#commonhashtags.pprint()
#commonhashtags.sortBy(lambda x:x)
#totalcount=tweet.updateStateByKey(aggregate_tweets_count)
#totalcount.pprint()
sorted_ = commonhashtags.transform(lambda rdd: rdd.sortBy(lambda x: (-x[1],x[0])))
#print(sorted_.collect())
#sorted_.pprint(3)
def ppprint(rdd):
	if(len(rdd.collect())!=0):
		print(rdd.collect()[0][0]+","+rdd.collect()[1][0]+","+rdd.collect()[2][0]+","+rdd.collect()[3][0]+","+rdd.collect()[4][0])
#tweet.pprint()
sorted_=sorted_.foreachRDD(ppprint)
ssc.start()
ssc.awaitTermination(25)
ssc.stop()
