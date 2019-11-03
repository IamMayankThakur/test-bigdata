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
	return y
def clean(x):
        hashtags = x.split(',')
        final =[]
        for h in hashtags:
            if(h == "" or h==" " or h=="  "):
                pass
            else:
                final.append(h)
        return final

def split(x):
	return (x,1)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

#ssc=StreamingContext(sc,float(sys.argv[2])) #take from the command line for batch interval
ssc=StreamingContext(sc,1)
ssc.checkpoint("file:///home/anagha/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
#dataStream = dataStream.window(float(sys.argv[1]), 1) #391
dataStream = dataStream.window(float(sys.argv[1]),float(sys.argv[2]))
tweet=dataStream.map(lambda x: x.split(';')[7])

tweet = tweet.flatMap(lambda line: clean(line))
tweet = tweet.map(split)

#window = tweet.reduceByKeyAndWindow(lambda x,y:x+y, int(sys.argv[1]),1)


window = tweet.reduceByKey(lambda x,y: x+y)


#totalcount=window.updateStateByKey(aggregate_tweets_count)


sorted_lex = window.transform(
    lambda rdd: rdd.sortBy(lambda x: x[0], ascending=True))
sorted_ = sorted_lex.transform(
    lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))


def display(c):
    
    lim = 0
    top5 = ""
    for w,count in c.collect():
        lim = lim +1
        if(lim ==5):
            top5= top5+ str(w).strip()#.encode('utf-8')).strip()
            break
        else:
            top5= top5+ str(w).strip()+ ","#.encode('utf-8')).strip() + "," #.encode('utf-8'))+ "," #.strip()) + ","
        #print(w.encode('utf-8'))
  
    print(top5)

sorted_.foreachRDD(display)

ssc.start()
#ssc.awaitTermination(12)
#time.sleep(12)
ssc.awaitTermination(60)
ssc.stop()
