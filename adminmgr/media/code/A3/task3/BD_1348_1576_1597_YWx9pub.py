import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(total_sum)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], count=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tweetid, count from hashtags order by count desc limit 3")
			hashtag_counts_df.show()
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)


def tmp1(x):
	temp = x.split(';')[7].split(',')
	return temp

def tmp2(x):
    return (x, 1)

class myClass:
	def outerFunc(self, x):	
		def innerFunc(rdd):	
			c=0	
			temp = rdd.take(6)
			for i in temp[:6]:		
				if(i[0]!=""):
					if(c==4):
						print(i[0])
						break
					print(i[0],end="")
					if(c<4):
						print(",",end="")
					c+=1
		x.foreachRDD(innerFunc)   
	def myjob(self, rdd):
		self.outerFunc(rdd)
	

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ws=int(sys.argv[1])
bd=int(sys.argv[2])

ssc=StreamingContext(sc,bd) #Batch/Slide duration
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.window(ws,1).flatMap(tmp1) #Window duration  #window(windowLength, slideInterval)
tweet1=tweet.map(tmp2)
count=tweet1.reduceByKey(lambda x,y:x+y)
count1=count.transform(lambda rdd: rdd.sortBy(lambda x: (-x[1], x[0])))# .filter(lambda x: x[0] is not '')

obj = myClass()
obj.myjob(count1)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
