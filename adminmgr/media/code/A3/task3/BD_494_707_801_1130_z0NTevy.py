import findspark
findspark.init()

import sys
import requests

from pyspark.sql.functions import lit
from pyspark.sql.functions import split
from pyspark.sql.functions import explode

from pyspark.sql import Row,SQLContext
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

def tweetCountaggr(val, sum):
	return sum(val) + (sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def solution(time, rdd):
	try:
		sql_context = get_sql_context_instance(rdd.context)
		row_val = rdd.map(lambda w: Row(tweetid=w))
		hash_val = sql_context.createDataFrame(row_val)
			
		wc_1=hash_val.select(explode(split(hash_val.tweetid,",")).alias("Hashtags"))
		wc_1.createOrReplaceTempView("modifications")
		
		wc_2= sql_context.sql("select Hashtags,count(*) from modifications group by Hashtags order by count(*) desc")
		
		final = wc_2.rdd
		final = final.sortBy(lambda x:x[0],True)
		final = final.sortBy(lambda x:x[1],False)
		
		count = 1

		for i,j in final.collect():
			if(i==''):
				continue
				
			elif(count==5):
				print(i+str(j),end='\n')

			elif(count<5):
				print(i+str(j),end=',')
			
			else:
				break

			count+=1
		
	except:
		err = sys.exc_info()[0]

def records(record):
	arr1=record.split(';')
	arr2=arr1[7].split(',')
	return arr1[7]
	
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
ssc=StreamingContext(sc,int(sys.argv[2]))
dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(records)
ttw=tweet.window(int(sys.argv[1]),1)
ttw.foreachRDD(solution)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()