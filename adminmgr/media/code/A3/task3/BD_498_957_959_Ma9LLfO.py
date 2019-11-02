import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,Window,SparkSession
import sys
import requests
import pprint

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
			hc=hashtag_counts_df.rdd.map(lambda x:x.tweetid).collect()
			print(','.join(hc))

		except:
			e = sys.exc_info()[0]



def process_rdd_new(time, rdd):
	try:
		new = rdd.sortBy(lambda x: (-x[1],x[0]))

		s=''
		k=0
		for i in new.collect():
			if k==4:
				s+=i[0]
				break
			s+=i[0]+','
			k+=1
		if k!=0:
			print(s)
	except:
		pass


def get_hashtag(x):
	a=x.split(';')[7]
	b=a.split(',')
	for i in b:
		if(i!=''):
			yield (i,1)



conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

spark = SparkSession.builder \
    .appName('appName') \
    .master('master') \
    .getOrCreate()

batch = int(sys.argv[2])
window_size = int(sys.argv[1])

ssc=StreamingContext(sc,1)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)

data=dataStream.window(window_size,batch).flatMap(get_hashtag).reduceByKey(lambda x,y : x+y)

data.foreachRDD(process_rdd_new)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
