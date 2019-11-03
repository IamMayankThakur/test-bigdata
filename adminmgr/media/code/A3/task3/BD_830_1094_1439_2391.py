import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)
def send_df_to_dashboard(df):
	# extract the hashtags from dataframe and convert them into array
	top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
	# extract the counts from dataframe and convert them into array
	tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
	for i in top_tags:
		print(i)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		#print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
			row_rdd = row_rdd.filter(lambda x : x[0] != '')
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			#hashtag_counts_df = sql_context.sql("select tweetid, no_of_tweets from hashtags")		
			hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc,hashtag limit 5")
		
			#hashtag_counts_df.show()
			send_df_to_dashboard(hashtag_counts_df)
			
		except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)

def tmp(x):
	return (x.split(';')[0],1)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
sc.setLogLevel("ERROR")

window = int(sys.argv[1])
batch = int(sys.argv[2])
ssc=StreamingContext(sc,batch)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
# dataStream.pprint()
tweet=dataStream.map(tmp)
# OR
hashtags=dataStream.map(lambda w:(w.split(';')[7],1))
#count=hashtags.updateStateByKey(aggregate_tweets_count)
count = hashtags.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y,window,2)

count.foreachRDD(process_rdd)
#count.pprint()

#TO maintain state
# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()

#To Perform operation on each RDD
# totalcount.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
