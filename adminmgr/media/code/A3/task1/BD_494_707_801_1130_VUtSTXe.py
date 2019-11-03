
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
	return y

def split(x):
	return (x,1)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,1) 
ssc.checkpoint("/checkpoint_BIGDATA")

tweet=ssc.socketTextStream("localhost",9009)
tweet = tweet.window(float(sys.argv[1]), float(sys.argv[2])) #391
tweet=tweet.map(tmp)

tweet = tweet.flatMap(lambda line: line.split(','))
tweet = tweet.map(split)


window = tweet.reduceByKey(lambda x,y: x+y)



sor_rdd = window.transform(
    lambda rdd: rdd.sortBy(lambda x: (-x[1], x[0]) , ascending=True))



def show(c):
    final=c.take(5)
    i= 0
    top5 = ""
    for w in final[:5]:
        i=i+1
        if(i ==5):
            top5= top5+ str(w)
            break
        else:
            top5 = top5+ str(w) + ","  
    print(top5)

sor_rdd.foreachRDD(show(c))




ssc.start()
#ssc.awaitTermination(12)
#time.sleep(12)
ssc.awaitTermination(60)
ssc.stop()
