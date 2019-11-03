import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def print_fun(time, rdd):

		#print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc, hashtag asc limit 5")
			#hashtag_counts_df.show()
			tags = hashtag_counts_df.rdd.map(lambda p: p.hashtag).collect()
			for tag in tags[:4]:
				print(tag, end=',')
			print(tags[4])
		except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,1)
ssc.checkpoint("/checkpoint_BIGDATA")

def f(iterator):
	for i in range(len(iterator)):
		if(iterator[i]!=''):
			yield iterator[i],1

lines=ssc.socketTextStream("localhost",9009)
words = lines.map(lambda line: line.split(";")[7].split(","))
words1=words.flatMap(f)

counts= words1.reduceByKeyAndWindow(lambda x, y: int(x) + int(y), lambda x, y: int(x) - int(y), int(sys.argv[1]), int(sys.argv[2]))
#counts.pprint()
counts.foreachRDD(print_fun)

ssc.start()
ssc.awaitTermination(60)
ssc.stop()
