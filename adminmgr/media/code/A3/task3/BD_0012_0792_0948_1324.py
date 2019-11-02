from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']
def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,2)
ssc.checkpoint("/home/mayur")

dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(lambda w:w.split(';')[7])
count=tweet.reduceByKeyAndWindow(lambda x,y:x+y ,lambda x,y:x-y ,16,8)
count.pprint()
ssc.start()
ssc.awaitTermination(65)
ssc.stop()
