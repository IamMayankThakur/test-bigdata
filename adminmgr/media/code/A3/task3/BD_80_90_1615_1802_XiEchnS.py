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
		print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(hashtag=w[0], no_of_tweets=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select hashtag,no_of_tweets from hashtags where hashtag <> ''  ORDER BY no_of_tweets DESC LIMIT 5")
			hashtag_counts_df.show()
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)

#def tmp(x):
	#return (x.split(';')[7])
if __name__ == "__main__":
	conf=SparkConf()
	conf.setAppName("BigData")
	sc=SparkContext(conf=conf)

	ssc=StreamingContext(sc,int(sys.argv[2]))
	ssc.checkpoint("./checkpoint_BIGDATA")

	dataStream=ssc.socketTextStream("localhost",9009)
	tweet=dataStream.map(lambda w: w.split(';')[7])
	tweet=tweet.flatMap(lambda w:w.split(','))
#dataStream.pprint()

# OR
	tweet=tweet.map(lambda w:(w,1))
	count=tweet.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,int(sys.argv[1]))
#count.pprint()

#TO maintain state
# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()

#To Perform operation on each RDD
	count.foreachRDD(process_rdd)

	ssc.start()
	ssc.awaitTermination(12)
	ssc.stop()
