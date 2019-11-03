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
			h_query = sql_context.sql("select hashtag, frequency from hashtags where hashtag<>'' order by frequency desc, hashtag asc limit 5")
			#hashtag_counts_df.show(5)
			hashtag_select = h_query.select("hashtag")
			hashtag_map = hashtag_select.rdd.map(lambda row : row[0])
			ll = hashtag_map.collect()
			

			
			print(','.join(ll))
			
		except:
			e = sys.exc_info()[0]


def split_map(x):
	a= x.split(';')
	a1=a[7]
	b= a.split(',')
	for y in b:
		yield (y,1)


conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

batch_size=int(sys.argv[2])

ssc=StreamingContext(sc,batch_size)
ssc.checkpoint("home/checkpoint_BIGDATA")


dataStream=ssc.socketTextStream("localhost",9009)

words_hash=dataStream.flatMap(split_map)


win=int(sys.argv[1])
count=words_hash.reduceByKeyAndWindow((lambda x,y:x+y),(lambda x,y:x-y),win,1)

count.foreachRDD(process_rdd)
#totalcount.pprint(5)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
