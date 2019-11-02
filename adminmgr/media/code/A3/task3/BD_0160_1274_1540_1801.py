#import findspark
#findspark.init()

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
		#print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			#row_rdd = rdd.map(lambda w: Row(tweetid=w[0]))
			#hashtags_df = sql_context.createDataFrame(row_rdd)
			#hashtags_df.registerTempTable("hashtags")
			#hashtag_counts_df = sql_context.sql("select tweetid from hashtags")
			#hashtag_counts_df.show()
			rdd = rdd.map(lambda w: w[0])
			rdd=list(rdd.collect())
			top_5=rdd[:5]
			for hashtag in top_5[:4]:
				print(hashtag,end=",")
			print(top_5[4])
			#print()
		except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)

def tmp(x):
	return (x.split(';')[0],1)

if len(sys.argv) != 3:
        print("Usage: pagerank <file>", file=sys.stderr)
        sys.exit(-1)

window_size=int(sys.argv[1])
batch_size=int(sys.argv[2])
#print(window_size,batch_size)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,batch_size) #batch size
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
# dataStream.pprint()
#tweet=dataStream.map(tmp)
# OR
tweet=dataStream.window(window_size,1).map(lambda w:w.split(';')[7].split(',')).flatMap(lambda x:x).map(lambda y:(y,1)).filter(lambda z:z[0] is not '')
#tweet.pprint()
count=tweet.reduceByKey(lambda x,y:x+y)
count_sorted_dstream = count.transform(lambda foo:foo.sortByKey().sortBy(lambda x:(-x[1])))
#top_five_authors = count_sorted_dstream.transform(lambda rdd:rdd.take(3))
#top_five_authors.pprint()
#top_3 = count_sorted_dstream.transform(lambda rdd: rdd.top(5))
#top_3.pprint()
#count_sorted_dstream.pprint()

#TO maintain state
# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()

#To Perform operation on each RDD
count_sorted_dstream.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
