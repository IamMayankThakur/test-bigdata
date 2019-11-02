import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import lit


def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		
	sql_context = get_sql_context_instance(rdd.context)
	#row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
	row_rdd = rdd.map(lambda w: Row(tweetid=w))
	hashtags_df = sql_context.createDataFrame(row_rdd)
	w1=hashtags_df.select(explode(split(hashtags_df.tweetid,",")).alias("hashtags"))
	#w1.show()
	w1.createOrReplaceTempView("updates1")
	w2 = sql_context.sql("select hashtags,count(*) from updates1 group by hashtags order by count(*) desc")
	#w2.show(3)
	r1=w2.rdd
	r1=r1.sortBy(lambda x:x[0],True)
	r1=r1.sortBy(lambda x:x[1],False)
	count=1
	for i,j in r1.collect():
		if(i==''):
			continue

		elif(count<5):
			print(i,end=',')
		elif(count==5):
			print(i,end='\n')
		else:
			break
		count=count+1
def tmp(x):

	temp=x.split(';')
	t1=temp[7].split(',')
	#print(t1)
	return temp[7]
	
	

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(tmp).window(int(sys.argv[1]),1)
tweet.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
