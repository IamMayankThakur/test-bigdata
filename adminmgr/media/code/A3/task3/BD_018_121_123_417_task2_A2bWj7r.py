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

def hashtag(n):
	x = n.split(";")[7]
	y = x.split(",")
	for i in y 	:
		if(i!=''):
			yield i
def process_rdd(time,rdd):
	try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0],num=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tweetid from hashtags limit 5")
			
			hashcount  = hashtag_counts_df.rdd.map(lambda x:x.tweetid).collect()
			print(','.join(hashcount))

	except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)

def empty_rdd():
	return
if __name__ == "__main__":
	if(len(sys.argv)!=3):
		sys.exit(-1)
	wind = int(sys.argv[1])
	batch = int(sys.argv[2])
	conf=SparkConf()
	conf.setAppName("BigData")
	sc=SparkContext(conf=conf)

	ssc=StreamingContext(sc,1)
	ssc.checkpoint("/checkpoint_BIGDATA")

	dataStream=ssc.socketTextStream("localhost",9009)

	tweet=dataStream.flatMap(hashtag)
	
	count_values_windowed = tweet.countByValueAndWindow(wind,batch)\
	                                .transform(lambda rdd:rdd\
	                                  .sortBy(lambda x:-x[1]))\
	                            .map(lambda x:(x[0],x[1]))
	
	count_values_windowed.foreachRDD(process_rdd)
	
	ssc.start()
	ssc.awaitTermination(60)
	ssc.stop()

