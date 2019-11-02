import findspark
findspark.init()
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,SparkSession
import sys
import requests
from operator import add



def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		#print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("has")
			#hashtags_df.show()
			#hc=sql_context.sql
			hashtag_counts_df = sql_context.sql("select tweetid,no_of_tweets from has order by no_of_tweets desc,tweetid asc limit 5")
			#,tweetid asc
			#print(hashtag_counts_df)
			#comment this out
			#hashtag_counts_df.show()
			result=hashtag_counts_df.collect()
			print("%s,%s,%s,%s,%s" % (result[0][0],result[1][0],result[2][0],result[3][0],result[4][0]))


		except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)





def aggregate_tweets_count(new_values,total_sum):
	return sum(new_values) + (total_sum or 0)


def has(x):
	if x=='':
		pass
	
	else:	
		r=[]
		for i in range(len(x)):
		
			r.append((x[i],1))
		return (r)



conf=SparkConf()
conf.setAppName("task2")
sc=SparkContext(conf=conf)


windowsize=int(sys.argv[1])
batchint=int(sys.argv[2])
slidingint=1*batchint

ssc=StreamingContext(sc,1)
ssc.checkpoint("/home/anjali/Desktop/sem5/BIGDATA/asn3/")
dataStream=ssc.socketTextStream("localhost",9009) 



tweet=dataStream.map(lambda x:(x.split(";")[7]))
tweet=tweet.flatMap(lambda x:(x.split(",")))
tweet=tweet.map(lambda x:(x,1))
tweet=tweet.filter(lambda x :x[0]!='')

#tweet=tweet.window(windowsize,batchint)

addfunc=lambda x,y:x+y
invaddfunc=lambda x,y: x-y
tags_totals=tweet.reduceByKeyAndWindow(addfunc,invaddfunc,windowsize,slidingint)
#tags_totals.pprint()


tags_totals.foreachRDD(process_rdd)


ssc.start()
#change this 12
ssc.awaitTermination(25)
ssc.stop()

