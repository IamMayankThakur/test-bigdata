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
		#print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(hashtag=w[0], frequency=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select hashtag, frequency from hashtags where hashtag<>'' order by frequency desc, hashtag asc limit 5")
			#hashtag_counts_df.show(5)			
			ll=hashtag_counts_df.select("hashtag").rdd.map(lambda row : row[0]).collect()
			
			print(','.join(ll))
			#hashtag_counts_df.sort("frequency")
			#rdd.show()
			#hashtag_counts_df.show()
		except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)

'''def tmp(x):
	a= x.split(';')[7]
	b= a.split(',')
	for j in b:
		yield (j,1)
'''
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

win=int(sys.argv[1])
batch_size=int(sys.argv[2])

ssc=StreamingContext(sc,batch_size)
ssc.checkpoint("home/chaitra/checkpoint_BIGDATA")


dataStream=ssc.socketTextStream("localhost",9009)
# dataStream.pprint()
#tweet=dataStream.flatMap(tmp)
# OR
tweet=dataStream.map(lambda l:l.split(";")[7])
words=tweet.flatMap(lambda t: t.split(","))
words_hash=words.map(lambda w: (w,1))
#words_hash.pprint()

count=words_hash.reduceByKeyAndWindow((lambda x,y:x+y),(lambda x,y:x-y),win,1)
#count.pprint(5)

#TO maintain state
#totalcount=words_hash.updateStateByKey(aggregate_tweets_count)
#totalcount.pprint()

#To Perform operation on each RDD
count.foreachRDD(process_rdd)
#totalcount.pprint(5)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
