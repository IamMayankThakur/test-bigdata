import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from pyspark.sql import window

def tmp(x):
	y = (x.split(';')[7]).split(',')
	return (y)

def forf(x):
	for i in x:
		yield (i,1)

def topprint(rdd):
	count=0
	for i in rdd.take(5):
		if(count==4):
			print("%s" % i)
		else:
			print("%s" % i,end=',')
		count = count +1

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		
		if(rdd.collect()):
			#print(sorted(rdd.collect(),reverse=True))
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tags=w[0], no_of_hashtags=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tags,no_of_hashtags from hashtags order by no_of_hashtags desc,tags asc")
			#hashtag_counts_df.show()
			hashrdd = hashtag_counts_df.rdd.map(lambda x: x.tags)
			topprint(hashrdd)
				
				
		




conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,1)
ssc.checkpoint("/checkpoint_BIGDATA")


#Selecting a datastream and then reducing by window:
#outpu2
dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(tmp)
septweet=tweet.flatMap(forf)
#septweet.pprint()
count=septweet.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,int(sys.argv[1]),int(sys.argv[2]))
tweet1=count.filter(lambda w:w[0] is not '')
#tweet1.pprint()
tweet1.foreachRDD(process_rdd)



ssc.start()
ssc.awaitTermination(25)
ssc.stop()
