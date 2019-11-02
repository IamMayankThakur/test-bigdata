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
	inter_hash_tags=x.split(';')[7]
	hash_tags=tuple(map(lambda x: (x,1),inter_hash_tags.split(',')))
	#print(hast_tags)
	return hash_tags
 
def col(x):
	x.collect()

def takeAndPrint(time, rdd):
	num=5	
	count=0	
	taken = rdd.take(num+25)
	for record in taken[:num+25]:
			
		#if(count==5):
		#	print("\n")
		#	break		
		if(record[0]!=""):
			if(count==4):
				print(record[0])
				break
			print(record[0],end="")
			if(count<4):
				print(",",end="")
			count+=1
	
	#if len(taken) > num:
	#	print("\n")
	
	        
	"""
        Print the first num elements of each RDD generated in this DStream.

        @param num: the number of elements from the first will be printed.
        """
	

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
#ssc=StreamingContext(sc,2)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
# dataStream.pprint()
tweet=dataStream.window(int(sys.argv[1]),1).map(tmp)
#tweet=dataStream.map(tmp)
tweet1=tweet.flatMap(lambda x:x)

#tweet1.pprint()

# OR
#tweet=dataStream.map(lambda w:(w.split(';')[0],1))
count=tweet1.reduceByKey(lambda x,y:x+y)
sort_t=count.transform(lambda rdd: rdd.sortBy(lambda x:(x[1],x[0]), ascending=False))
#sort_t.pprint(20)
#print(type(sort_t))
sort_t.foreachRDD(takeAndPrint)

	
#for i in sort_t.pprint():
#	print(i[0],end='',sep=',')
#count.pprint()

#TO maintain state
# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()

#To Perform operation on each RDD
# totalcount.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()

'''
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
		print("----------=========- %s -=========----------" % str(time))
		#print(rdd.collect())
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			print(row_rdd.collect())
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tweetid, no_of_tweets from hashtags")
			hashtag_counts_df.show()
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)

def tmp(x):
	hash_t=x.split(';')[7]
	for i in hast_t.split(','):
		yield (i,1)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,2)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",12001)
#dataStream.pprint()
tweet=dataStream.map(tmp)
tweet.pprint()
# OR
#tweet=dataStream.map(lambda w:(w.split(';')[0],1))
count=tweet.reduceByKey(lambda x,y:x+y)
#count.pprint()

#TO maintain state
totalcount=tweet.updateStateByKey(aggregate_tweets_count)
#totalcount.pprint()

#To Perform operation on each RDD
totalcount.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(12)
ssc.stop()
'''
