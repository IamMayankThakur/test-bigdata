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

def process_rdd(time, rdd):
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tweetid from hashtags limit 5")
			#hashtag_counts_df.show()
			#print("---------------------------")
			#row1 = hashtag_counts_df.collect()[4]
			#print(row1)
			hc = hashtag_counts_df.rdd.map(lambda x:(x.tweetid).collect()
			print(','.join(hc))
			#hc=hashtag_counts_df.collect()
			#hashtag_counts_df.show()

		except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)

def tmp(x):
	return (x.split(';')[0],1)
def get_hashtag(x):
	hashtaggrp=x.split(';')[7]
	each_hashtag=hashtaggrp.split(',')
	for i in each_hashtag:
		if(i!=''): #empty string
			yield (i,1)
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

# 1 is batch interval 
ssc=StreamingContext(sc,1))
ssc.checkpoint("/checkpoint_BIGDATA")

#Each record in this DStream is a line of text
dataStream=ssc.socketTextStream("localhost",9009)


#window(windowLength, slideInterval)
data = dataStream.window(int(sys.argv[1]),int(sys.argv[2]) 
words=data.flatMap(get_hashtag)

totalcount=words.reduceByKey(lambda x,y:x+y)
sortedbykey = totalcount.transform(lambda rdd: rdd.sortBy(lambda x:x[0]))
popular = sortedbykey.transform(lambda rdd: rdd.sortBy(lambda x:x[1],ascending = False))
popular.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()

