import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from pyspark.sql import window
def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		#print("----------=========- %s -=========----------" % str(time))
		try:
			if(rdd.collect()):
				#print(sorted(rdd.collect(),reverse=True))
				sql_context = get_sql_context_instance(rdd.context)
				row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
				hashtags_df = sql_context.createDataFrame(row_rdd)
				hashtags_df.registerTempTable("hashtags")
				hashtag_counts_df = sql_context.sql("select tweetid,no_of_tweets from hashtags order by no_of_tweets desc,tweetid asc limit 5")
				#hashtag_counts_df.show()
				sample2 = hashtag_counts_df.rdd.map(lambda x: x.tweetid)
				print(",".join(sample2.collect()))
				
				
		except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)

def tmp(x):
	y = x.split(',')

def splithash(x):
    y=x.map(lambda i : (i.split(',')),1)
    return y

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
batch_interval = int(sys.argv[2])
batch_duration = int(sys.argv[1])
ssc=StreamingContext(sc,1)
ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)

count=dataStream.map(lambda w:(w.split(';')[7]))
tweet1 = count.flatMap(lambda line: line.split(","))
tweet = tweet1.map(lambda word:(word, 1))
slide=batch_interval
windowedCounts = tweet.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,batch_duration,slide).filter(lambda x:x[0] is not '')

windowedCounts.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
