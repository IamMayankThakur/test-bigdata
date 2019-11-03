import findspark
findspark.init()
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
def tmp(tweet):
	t = tweet.split(';')
	for i in t[7].split(','):
		if len(i)>2:
			yield(i,1)
def printfunc(r1):
	r2 =sorted(r1,key = lambda x:x[0])
	res =sorted(r2,key = lambda x:x[1],reverse = True)
	s = ""
	for i in range(5):
		s+= res[i][0]+","
	s1 = s[:len(s)-1]
	print(s1)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']
def process_rdd(time, rdd):
		try:
			r1 = rdd.collect()
			if(len(r1)>2):
				printfunc(r1)
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)
if __name__ == "__main__":

	conf=SparkConf()
	conf.setAppName("BigData")
	sc=SparkContext(conf=conf)
	ssc=StreamingContext(sc,int(sys.argv[2]))
	ssc.checkpoint("/checkpoint_BIGDATA")  

	dataStream=ssc.socketTextStream("localhost",9009)  
    
	hashtag = dataStream.flatMap(tmp)
    h = hashtag.window(int(sys.argv[1]),1)
	h1 = h.reduceByKey(lambda x,y:x+y)
    
	h1.foreachRDD(process_rdd)
    
	ssc.start()
	ssc.awaitTermination(60)
	ssc.stop()



