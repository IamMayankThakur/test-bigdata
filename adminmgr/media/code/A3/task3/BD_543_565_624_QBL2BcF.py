#import findspark findspark.init()

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
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
#tweet=dataStream.filter(lambda w: w.split(';')[7].strip())
#tweet.pprint()
#dataStream.pprint()
tweet=dataStream.map(lambda w:w.split(';')[7].strip())
#tweet=tweet.flatMap(lambda w:list(map(lambda x:x,w.split(','))))
tweet=tweet.flatMap(lambda x:x.split(','))
#tweet=tweet.filter(lambda x:x!='')
#tweet.pprint(5)
tweet=tweet.map(lambda x:(x,1))
#tweet.pprint(5)
#tweet.pprint()
#tweet1=tweet.countByValue()
#tweet=tweet.map(lambda w:w[0])
#hi=tweet.map(lambda
#tweet.pprint()
tweet= tweet.window(int(sys.argv[1]), 1)
commonhashtags = tweet.reduceByKey(lambda x,y:x+y)
#commonhashtags.pprint()
#commonhashtags.sortBy(lambda x:x)
#totalcount=tweet.updateStateByKey(aggregate_tweets_count)
#totalcount.pprint()
sorted_ = commonhashtags.transform(lambda rdd: rdd.sortBy(lambda x: (-x[1],x[0])))
#print(sorted_.collect())
#sorted_.pprint(5)
def ppprint(rdd):
#	if(len(rdd.collect())!=0):
		res=''
		c=0
		for i in rdd.collect():
			if(i[0]!=''):
				res=res+i[0]+","
				c=c+1
			if(c==5):
				break
	#	print(rdd.collect()[0][0]+","+rdd.collect()[1][0]+","+rdd.collect()[2][0]+","+rdd.collect()[3][0]+","+rdd.collect()[4][0])
	#	if(res!=''):
		print(res[:len(res)-1])
#tweet.pprint()
sorted_1=sorted_.foreachRDD(ppprint)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
