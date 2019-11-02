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
	return (y)

def split(x):
	return (x,1)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2])) #take from the command line for batch interval
#ssc=StreamingContext(sc,1)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)

tweet=dataStream.map(tmp)
tweet = tweet.flatMap(lambda line: line.split(","))
tweet = tweet.map(split)
#tweet = tweet.map(lambda w: [w.split(',')[i], 1] 
#tweet.pprint()

#window length, sliding interval
#window = tweet.reduceByKeyAndWindow(lambda x,y:x+y, int(sys.argv[1]),1)

window1 = tweet.window(int(sys.argv[1]), 1)
window = window1.reduceByKey(lambda x,y: x+y)


#totalcount=window.updateStateByKey(aggregate_tweets_count)


sorted_lex = window.transform(
    lambda rdd: rdd.sortBy(lambda x: x[0], ascending=True))
sorted_ = sorted_lex.transform(
    lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))


def display(c):
    lim = 0
    top5 = ""
    for count in c.collect():
        lim = lim +1
        if(lim ==5):
            top5= top5+ str(count[0])
            break
        else:
            top5 = top5+ str(count[0]) + ","
  
    print(top5)

sorted_.foreachRDD(lambda c: display(c))




ssc.start()
#ssc.awaitTermination(12)
#time.sleep(12)
ssc.awaitTermination(25)
ssc.stop()
