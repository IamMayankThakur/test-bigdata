#import findspark
#findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from operator import add
hashtags = []
count = 0
def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank)

def printCommonHashtags(rdd):
	#filter the rdd to remove all empty tags then reduce by key to combine hastags of all the same sources into one comma separated rdd
	tweet=rdd.filter(lambda w: w[1]!='').reduceByKey(lambda x,y:x+','+y)
	#take each comma separated value and use flatmap to divide contribution amongst all as 1
	#then reduce by key and find sum
	hashtags = tweet.map(lambda x: (x[1],1)).flatMap(lambda x: computeContribs(x[0].split(","),x[1])).reduceByKey(add)
	#sort the rdd in descending order
	sorttable= hashtags.sortBy(lambda x:x[1],ascending=False)
	#print the values of the first three common hashtags
	for (hashtag,count) in sorttable.collect()[:2]:
		print(hashtag,',',sep='',end='')
	for (hashtag,count) in sorttable.collect()[2:3]:
		print(hashtag,sep='')
	

def printHash(rdd):
	global count
	global hashtags
	for (hashtag) in rdd.collect():
		if(count == 4):
			print(hashtag,sep='',end='')
			count += 1
		if(count != 5):
			print(hashtag,',',sep='',end='')
			count += 1
		else:
			print()
			count = 0
			return


conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("~/Desktop/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(lambda w:(w.split(';')[3],w.split(';')[7]))
wind=tweet.window(int(sys.argv[1]),1) #Sliding interval set to 1 
filtered=wind.filter(lambda w: w[1]!='').reduceByKey(lambda x,y:x+','+y)
hashtags = filtered.map(lambda x: (x[1],1)).flatMap(lambda x: computeContribs(x[0].split(","),x[1])).reduceByKey(add)
sortedtags=hashtags.transform(lambda x: x.sortBy(lambda y:(-y[1],y[0]))).map(lambda x: x[0])


sortedtags.foreachRDD(lambda x : printHash(x))
#sortedtags.pprint(3)
#tweet.foreachRDD(lambda x : printCommonHashtags(x))

ssc.start()
ssc.awaitTermination(25)
ssc.stop()

