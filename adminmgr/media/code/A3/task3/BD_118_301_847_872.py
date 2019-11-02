#import findspark
#findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import time
import pprint
#import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def tmp(x):
	y = x.split(';')[7];
	#z= y.split(',')
	#for el in z:
	return (y)

def split(x):
	return (x,1)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2])) #take from the command line for batch interval
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
#tweet=dataStream.filter(lambda w:"Android" in w.split(';')[3])
tweet=dataStream.map(tmp)
tweet = tweet.flatMap(lambda line: line.split(","))
tweet = tweet.map(split)
#tweet = tweet.map(lambda w: [w.split(',')[i], 1] 
#tweet.pprint()

window = tweet.reduceByKeyAndWindow(lambda x,y:x+y, int(sys.argv[1]), 1)
#totalcount=window.updateStateByKey(aggregate_tweets_count)
#totalcount.pprint()
'''def parse(line):
    try:
        k, v = line.split(",")
        yield (k, int(v))
    except ValueError:
	print("heeellooo")'''
#parsed = totalcount.flatMap(lambda w: parse(line))
#parsed.pprint()

sorted_lex = window.transform(
    lambda rdd: rdd.sortBy(lambda x: x[0], ascending=True))
sorted_ = sorted_lex.transform(
    lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

#window length, sliding interval
#in the 7th index, comma separated hashtags
#sorted_.pprint(3)


tags = sorted_.transform(lambda rdd: rdd.keys() ) 
final = tags.transform(lambda rdd: rdd.take(3))
final.pprint(3)
#tags.foreachRDD(lambda rdd: rdd.pprint())
#tags = tags.transform(lambda rdd: rdd.collect()) 
#print(tags)
#tags.flatMap(process)
#print(tags)

#for i in range (0,3):  
#final = sorted_.filter( lambda rdd: rdd.filter(lambda x: x[0]))
#final.pprint(3)
'''x = 0
for i in sorted_.keys.collect:
    x = x+1
    print(str(i)) #, end= ",")
    if(x == 3):
        break'''


ssc.start()
#ssc.awaitTermination(12)
time.sleep(12)
ssc.stop()
