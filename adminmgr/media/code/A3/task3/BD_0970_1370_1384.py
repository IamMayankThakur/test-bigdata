import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
def compute(hashtag):
	tags = hashtag.split(',')
	for h in tags:
		if(len(h)>1):
			yield (h,1)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.sortBy(lambda a : -a[1])
			r1 = row_rdd.collect()
			#print(r1)
			if(len(r1)>2):
				print(r1[0][0],",",r1[1][0],",",r1[2][0],",",r1[3][0],",",r1[4][0])
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)

if __name__ == "__main__":

	conf=SparkConf()
	conf.setAppName("BigData")
	sc=SparkContext(conf=conf)

	ssc=StreamingContext(sc,int(sys.argv[2])) #2 is batch durtion
	ssc.checkpoint("./checkpoint_BIGDATA") #checkpoint is for recovering the lost data 

	dataStream=ssc.socketTextStream("localhost",9009)  
	
	tweet=dataStream.map(lambda w:(w.split(';')[7]))
	#tweet.pprint()
	hashtag = tweet.flatMap(lambda w :compute(w))
	#hashtag.pprint()
	count=hashtag.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, int(sys.argv[1]))
	#count.pprint()

	#To Perform operation on each RDD
	count.foreachRDD(process_rdd)
	
	ssc.start()
	ssc.awaitTermination(25)
	ssc.stop()
