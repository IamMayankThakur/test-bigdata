import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']
def sorting(rank):
   return sorted(rank,key=lambda n:(-n[1],n[0]))
def process_rdd(time, rdd):
	#temp = sorted(rdd.collect(),key = lambda n:n[1])
	temp = sorting(rdd.collect())
	counter = 0
	string = ""
	var = 5
	for i in temp:
		if (counter == var):
			break
		if(temp[counter][0] == ""):
			var+=1		
		if(counter == 0 and temp[counter][0] != ""):
			string = temp[counter][0]
		elif (temp[counter][0] != ""):		
			string = string +','+ temp[counter][0]
		counter+=1
	if(len(string) > 0):
		print(string)
def tmp(x):
	for i in (x.split(';')[7]).split(','):
		return (i,1)

windowDuration = int(sys.argv[1])
slideInterval = int(sys.argv[2])

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,1)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
temp = dataStream.window(windowDuration,slideInterval)
# dataStream.pprint()
tweet=temp.flatMap(lambda x:((x.split(';')[7]).split(",")))
tweet = tweet.map(lambda x:(x,1))
# OR
#tweet=dataStream.map(lambda w:(w.split(';')[0],1))
count=tweet.reduceByKey(lambda x,y:x+y)
#count.pprint()

#TO maintain state
totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()
#To Perform operation on each RDD
totalcount.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
