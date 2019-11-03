#import findspark
#findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import time
import pprint


def emp(x):
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

ssc=StreamingContext(sc,1) #take from the command line for batch interval

ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
dataStream = dataStream.window(float(sys.argv[1]), float(sys.argv[2])) #391
tweet=dataStream.map(lambda h: h.split(';')[7])

tweet = tweet.flatMap(lambda line: emp(line))
tweet = tweet.map(lambda h: (h,1))

#window = tweet.reduceByKeyAndWindow(lambda x,y:x+y, int(sys.argv[1]),1)


window = tweet.reduceByKey(lambda x,y: x + y)


#totalcount=window.updateStateByKey(aggregate_tweets_count)


sorted_lex = window.transform(
    lambda rdd: rdd.sortBy(lambda x: x[0], ascending=True))
sorted_ = sorted_lex.transform(
    lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))


def display(c):
    lim = 0
    for h,word in c.collect():
        lim = lim +1
        if(lim ==5):
            print(h, end = "\n")
            break
        else:
            print(h, end = ",")

           
  
  

sorted_.foreachRDD(display)
ssc.start()
#ssc.awaitTermination(12)
#time.sleep(12)
ssc.awaitTermination(25)
ssc.stop()
