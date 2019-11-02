import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,Window
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		#print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tweetid from hashtags limit 5")
			hc=hashtag_counts_df.rdd.map(lambda x:x.tweetid).collect()
			print(','.join(hc))

		except:
			e = sys.exc_info()[0]


def tmp(x):
	return (x.split(';')[0],1)

def get_hashtag(x):
	a=x.split(';')[7]
	b=a.split(',')
	for i in b:
		if(i!=''):
			yield (i,1)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)


batch = int(sys.argv[2])
window_size = int(sys.argv[1])

ssc=StreamingContext(sc,batch)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)

tweet=dataStream.map(tmp)

data=dataStream.window(window_size,1).flatMap(get_hashtag).reduceByKey(lambda x,y:x+y).transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

data.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
