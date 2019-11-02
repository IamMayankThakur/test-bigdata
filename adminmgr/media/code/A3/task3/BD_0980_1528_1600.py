import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from operator import add

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
			row_rdd = rdd.map(lambda w: Row(hashtag=w[0],hashtag_count=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select hashtag,hashtag_count from hashtags where hashtag like '_%' order by hashtag_count desc limit 5")
			#hashtag_counts_df.show()
			send_df_to_dashboard(hashtag_counts_df)
		except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)

def send_df_to_dashboard(df):
	#top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
	top_tags = ((str(x[0]),x[1]) for x in df.select('*').collect())
	sortedIterms = sorted(top_tags,key=lambda x:(-x[1],x[0]))
	k=0
	for i in sortedIterms:
		k+=1
		if(k<5):
			print(i[0],end=",")
		else:
			print(i[0])
			k=0

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
ssc=StreamingContext(sc,int(sys.argv[2]))

ssc.checkpoint("/checkpoint_BIGDATA")
dataStream1=ssc.socketTextStream("localhost",9009)
dataStream = dataStream1.window(int(sys.argv[1]),1)
words = dataStream.flatMap(lambda line: ((line.split(";")[7]).split(",")))
hashtags = words.map(lambda x: (x, 1))
#tags_totals=hashtags.updateStateByKey(aggregate_tweets_count)
tags_totals=hashtags.reduceByKey(add)
tags_totals.foreachRDD(process_rdd)

ssc.start() 
ssc.awaitTermination(25)
ssc.stop()
