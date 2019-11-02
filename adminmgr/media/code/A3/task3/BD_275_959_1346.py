"""import findspark
findspark.init()
import sys
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
		print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tweetid, no_of_tweets from hashtags")
			hashtag_counts_df.show()
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)

def tmp(x):
	return (x.split(';')[0],1)
if(len(sys.argv) != 3):
	print("Usage: bin/submit-spark <filename> <window size> <batch duration>", file=sys.stderr)
	sys.exit(-1)

win_siz = int(sys.argv[1])
bat_dur = int(sys.argv[2])
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,2)
ssc.checkpoint("/home/chaitra/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
# dataStream.pprint()
tweet=dataStream.map(tmp)
# OR
tweet=dataStream.map(lambda w:(w.split(';')[0],1))
count=tweet.reduceByKey(lambda x,y:x+y)
count.pprint()

#TO maintain state
# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()

#To Perform operation on each RDD
# totalcount.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(12)
ssc.stop()"""

# import findspark
# findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)
if(len(sys.argv) != 3):
	print("Usage: bin/submit-spark <filename> <window size> <batch duration>", file=sys.stderr)
	sys.exit(-1)

win_siz = int(sys.argv[1])
bat_dur = int(sys.argv[2])
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,bat_dur)
ssc.checkpoint("~/Desktop/checkpoint_BIGDATA")
def tmp(x):
	x.pprint()
	return (x.split(';')[0],1)
dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(tmp)
tweet=tweet.window(win_siz, 2)

def fun(x):
	h = x.split(";")[7]
	h = h.split(",")
	for i in h:
		yield (i, 1)

hashtags=dataStream.flatMap(fun)
hashtags=hashtags.filter(lambda x:False if x[0] == "" else True)
count=hashtags.reduceByKey(lambda x,y:x+y)

count = count.transform(lambda rdd: rdd.sortBy(lambda a: (-a[1], a[0])))
#count = count.transform(lambda rdd: sc.parallelize(rdd.take(3)))

def fun1(rdd):
	r = rdd.collect()
	if(len(r) < 5):
		return
	for i in range(5):
		if(i == 4):
			print(r[i][0])
			
		else:
			print(r[i][0], end = ",")
count.foreachRDD(fun1)

	

# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()

ssc.start()
ssc.awaitTermination(12)
ssc.stop()
