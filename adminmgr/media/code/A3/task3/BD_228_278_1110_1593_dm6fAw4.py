# import findspark
# findspark.init()
from __future__ import print_function
from pyspark.sql import SparkSession
#from pyspark import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import *
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
#import requests
def explode_cu(hasht):
	for i in hasht:
		yield i

def print_rdd(foo):
	cnt=0
	for i in foo.collect():
		if(i[0]!=''):
			if(cnt!=4):
				print(i[0],end=",")
				cnt+=1
			else:
				print(i[0])
				cnt+=1
		if(cnt==5):
			break
	#print("\b")
#def aggregate_tweets_count(new_values, total_sum):
#return sum(new_values) + (total_sum or 0)

if __name__=="__main__":
	win_size=int(sys.argv[1])
	win_dur=int(sys.argv[2])
	
conf=SparkConf()
conf.setAppName("Task2")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,win_dur)
ssc.checkpoint("checkpoint")

dataStream=ssc.socketTextStream("localhost",9009)

#To collect all the hashtags of a particular tweet
hashtag=dataStream.map(lambda w: w.split(';')[7])
#To make each hashtag as a unique identity
hashtags=hashtag.flatMap(lambda t:explode_cu(t.split(',')))
'''hashtags1=list_hashtags.map(lambda t:map(lambda x:x,t))
hashtags=hashtags1.map(lambda t:map(lambda x:x,t))'''
#Counts the number of times the particular hastag was used
ht_count=hashtags.countByValueAndWindow(win_size,1)
#Sorts in descending order
ht_count=ht_count.transform(lambda foo:foo.sortBy(lambda k:(-k[1],k[0])))
'''top_three=ht_count.transform(
  lambda rdd: sc.parallelize(rdd.take(3))
)
top_three.pprint()'''
#print("%s,%s,%s"%(top_three[0],top_three[1],top_three[2]))
#ht_count.pprint()
ht_count.foreachRDD(lambda foo:print_rdd(foo))

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
