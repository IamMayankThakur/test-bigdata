import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tags_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def printdata(time,rdd):
	try:
		sql_context=get_sql_context_instance(rdd.context)
		row_rdd=rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
		hashtags_df = sql_context.createDataFrame(row_rdd)
		hashtags_df.registerTempTable("hashtags")
		hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc,hashtag limit 5")
		# hashtag_counts_df.show()
		temp=hashtag_counts_df.collect()
		windowoutput=''
		for i in temp:
			if(windowoutput!=''):
				windowoutput=windowoutput+','+i[0]
			else:
				windowoutput=windowoutput+i[0]
		windowoutput=windowoutput
		print(windowoutput)
	except Exception as e:
		pass

conf=SparkConf()
conf.setAppName("TestFakeData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("/checkpoint_FAKEDATA")

dataStream = ssc.socketTextStream("localhost",9009)
hashtags = dataStream.map(lambda w:w.split(';')[7])

hashtag = hashtags.flatMap(lambda w:w.split(','))
hashtag = hashtag.filter(lambda x:len(x)!=0)

countoftags = hashtag.map(lambda x: (x, 1))
countoftags = countoftags.reduceByKey(lambda x,y:x+y)

window = countoftags.reduceByKeyAndWindow(lambda x,y:x+y,None,int(sys.argv[1]),1)
window.foreachRDD(printdata)

ssc.start()
ssc.awaitTermination(60)
ssc.stop()