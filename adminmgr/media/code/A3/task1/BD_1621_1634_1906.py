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
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			row_rdd=rdd.sortBy(lambda w:(-w[1],w[0]))
			
			count1=0
			g=""
			for (tweet,count) in row_rdd.collect():
				
				if(count1 <5):
					#if(tweet!=""):
					g=g+","+str(tweet)
					count1=count1+1
			print(g.split(",",1)[1])
			
			
			
			
		except:
			e = sys.exc_info()[0]
			
def sort1(x):
	x.sort()
	return x[0]

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("/checkpoint_BIGDATA3")

dataStream=ssc.socketTextStream("localhost",9009)

tweet1=dataStream.flatMap(lambda w:((w.split(';')[7]).split(',')))
tweet3=tweet1.map(lambda w:(w,1))

#count=tweet3.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x+y,int(sys.argv[1])).filter(lambda x:(x[0]!=""))
count=tweet3.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,int(sys.argv[1])).filter(lambda x:(str(x[0])!="" and int(x[1])!=0))
count.pprint()

count.foreachRDD(process_rdd)




ssc.start()
ssc.awaitTermination(25)
ssc.stop()
