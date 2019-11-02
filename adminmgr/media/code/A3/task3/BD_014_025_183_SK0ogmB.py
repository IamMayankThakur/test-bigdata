import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
from operator import add
import requests


def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], hash_tags=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select hash_tags, count(hash_tags) as `counter` from hashtags group by hash_tags")
			
			hashtag_counts_df.show()
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)
def echo(time, rdd):
	counts = rdd.collect()
	if(len(counts)!=0):
		print(counts[0][0],",",counts[1][0],",",counts[2][0],",",counts[3][0],",",counts[4][0],sep="")
def tmp(x):
	splitter = x.split(';')
	for hashtags in splitter[7].split(','):
		if(hashtags != ''):
			yield (hashtags,1)

conf=SparkConf()
conf.setAppName("BigDataAssignment3")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("/home/chaitra/BigData/HandsOn3/Ass3/Checkpoints")

dataStream = ssc.socketTextStream("localhost",9009)
newStream = dataStream.window(int(sys.argv[1]),1).flatMap(tmp)
finalStream = newStream.reduceByKey(add)
sortedStream = finalStream.transform((lambda foo:foo.sortBy(lambda x:(-x[1],x[0]))))
top_three_hash = sortedStream.transform(lambda rdd:rdd.context.parallelize(rdd.take(5)))
top_three_hash.foreachRDD(echo)
ssc.start()
ssc.awaitTermination(25)
ssc.stop()
