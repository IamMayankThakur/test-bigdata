#import findspark
#findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def Sort_Tuple(tup):  
  
    # reverse = None (Sorts in Ascending order)  
    # key is set to sort using second element of  
    # sublist lambda has been used  
    tup.sort(key=lambda x:x[0])
    tup.sort(key = lambda x: x[1],reverse=True)  
    return tup  
  
def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		rdd1 = rdd.collect()
		l=Sort_Tuple(rdd1)
		c=0
		hashtag=[]
		for i in l:
			if(c<5):
				if(i[0] !=''):
					hashtag.append(i[0])
					c=c+1
		print(hashtag[0],',',hashtag[1],',',hashtag[2],',',hashtag[3],',',hashtag[4])



		'''
		c=0
		i=0
		while(c<5):
			if(l[i][0] != ''):
				print(l[i][0])
				c=c+1
			i=i+1
			
			
		
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
		'''

def tmp(x):
	a=x.split(';')[7]
	b=a.split(',')
	for i in b:
		return (i,1);
		

ws = sys.argv[1]
bd = sys.argv[2]

bd = int(bd)
ws = int(ws)
'''
print(bd)
print(ws)
b=2
b=int(b)
w=2
w=int(w)
'''
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,bd) #(spark_obj , batchDuration in seconds)...cannot be 1 if 1, then it creates empty list for the first rdd (bcoz of the fact that both the reading interval and the batch durations being same i.e 1 second each...mentioned in piazza and also new app.py has 500 rows sent "per second" ) and hence this code throws broken pipe error
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
# dataStream.pprint()
tweet=dataStream.map(tmp)
# OR
#tweet=dataStream.map(lambda w:(w.split(';')[0],1))
count=tweet.reduceByKeyAndWindow(lambda x, y: int(x) + int(y) ,lambda x,y:int(x)-int(y), ws) # window size (in seconds) should be a multiple of batch duration and hence cannot be 3/5/7 etc in this case, and the second paramter is a inverse function which ensures the compuatation is not redudant and the sliding interval is a default value which is 1 
#count.pprint()

#TO maintain state
totalcount=tweet.updateStateByKey(aggregate_tweets_count)
#totalcount.pprint()

#To Perform operation on each RDD
totalcount.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
