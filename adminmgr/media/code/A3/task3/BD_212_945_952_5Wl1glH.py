
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


def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
	try:
		
		if(rdd.collect()):
			#print(sorted(rdd.collect(),reverse=True))
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tags=w[0], no_of_hashtags=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tags,no_of_hashtags from hashtags order by no_of_hashtags desc,tags asc limit 5")
			#hashtag_counts_df.show()
			hashrdd = hashtag_counts_df.rdd.map(lambda x: x.tags)
			#topprint(hashrdd)
			print(",".join(hashrdd.collect()))			
	except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)		
				
		



conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,1)
ssc.checkpoint("/checkpoint_BIGDATA")

#Try in outpu1
inputStream=ssc.socketTextStream("localhost",9009)
dataStream = inputStream.window(int(sys.argv[1]),int(sys.argv[2])) 
tweet=dataStream.map(tmp)
septweet=tweet.flatMap(forf)
count=septweet.reduceByKey(lambda x,y:x+y)
#sortcount = count.transform(lambda rdd :rdd.sortBy(lambda a:a[0],ascending=True))
#sortcount1 = sortcount.transform(lambda rdd :rdd.sortBy(lambda a:a[1],ascending=False))
#tweet1=sortcount1.filter(lambda w:w[0] is not '')
#tweet1.pprint()
#res = tweet1.map(lambda a : a[0])
#res.foreachRDD(rddprint)
res = count.filter(lambda w:w[0] is not '')
res.foreachRDD(process_rdd)




ssc.start()
ssc.awaitTermination(25)
ssc.stop()
