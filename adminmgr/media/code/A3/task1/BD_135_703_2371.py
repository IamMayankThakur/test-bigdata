import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


'''
def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']
def process_rdd(time, rdd):

    	# Get spark sql singleton context from the current context
    	sql_context = get_sql_context_instance(rdd.context)
    	# convert the RDD to Row RDD
    	row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
    	# create a DF from the Row RDD
    	hashtags_df = sql_context.createDataFrame(row_rdd)
    	# Register the dataframe as table
    	hashtags_df.registerTempTable("hashtags")
    	# get the top 10 hashtags from the table using SQL and print them
    	hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 5")
    	hashtag_counts_df.show()
'''
	
def sortandprint(rdd):
	sorted_rdd1 = rdd.sortBy(lambda x: (x[1],x[0]))
	sorted_rdd=sorted_rdd1.filter(lambda y: y[0] !='')
	s_list=sorted_rdd.collect()
	if(s_list!=[]):
		print(s_list[0][0],s_list[1][0],s_list[2][0],s_list[3][0],s_list[4][0],sep=",")	
		
    



def aggregate_hashtags_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
hashtag1=dataStream.window(int(sys.argv[1]),1)

if(',' in hashtag1.select(lambda w: w.split(";")[7])):
	hashtag2=hashtag1.select(lambda w: w.split(";")[7])
	hashtag3=hashtag2.flatmap(lambda p:p.split(","))
else:
	hashtag3=hashtag1.flatmap(lambda w: w.split(";")[7])
hashtag4 = hashtag3.map(lambda x: (x, 1))
#hashtags=hashtag4.reduceByKey(add)
hashtags=hashtag4.updateStateByKey(aggregate_hashtags_count)
hashtags.foreachRDD(func)

totalcount=hashtags.updateStateByKey(aggregate_hashtags_count)
totalcount1=totalcount.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,sysargv[1],sysargv[2])
totalcount1.pprint()

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
