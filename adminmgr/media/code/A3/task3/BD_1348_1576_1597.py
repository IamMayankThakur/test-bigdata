import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(total_sum)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0][0]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			r=rdd.collect()
			l=[]
			for i in range(5):
    				l.append(r[i][0])
			print(','.join(l))
		except:
			e = sys.exc_info()[0]


def tmp1(x):
	temp = x.split(';')[7].split(',')
	return temp

def tmp2(x):
    return (x, 1)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ws=int(sys.argv[1])
bd=int(sys.argv[2])

ssc=StreamingContext(sc,bd) #Batch/Slide duration
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.window(ws,1).flatMap(tmp1) #Window duration  #window(windowLength, slideInterval)
tweet1=tweet.map(tmp2)
count=tweet1.reduceByKey(lambda x,y:x+y)
count1=count.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False)).filter(lambda x: x[0] is not '')
count1.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
