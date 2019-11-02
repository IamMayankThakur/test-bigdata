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
			rdd = rdd.filter(lambda b : b[0] != '')
			row_rdd = rdd.map(lambda q: Row(hashtag=q[0], hashtag_count=q[1]))
			
			famous_tags = row_rdd.collect()		
			
			res = sorted(famous_tags, key = lambda x:(-x[1],x[0]))
			if(len(res) > 4):
				print(res[0][0]+','+res[1][0]+','+res[2][0]+','+res[3][0]+','+res[4][0])
	
					


			
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

tweet=dataStream.map(tmp)

hashtags=dataStream.flatMap(lambda w: w.split(';')[7].split(','))
hashtags1 = hashtags.map(lambda w:(w,1))
count = hashtags1.reduceByKeyAndWindow(lambda a, b: a + b, lambda a, b: a - b,window,1)

count.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
