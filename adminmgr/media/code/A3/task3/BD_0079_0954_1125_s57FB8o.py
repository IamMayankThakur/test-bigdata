import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from operator import add

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
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

def tmp(x):
    for i in x.split(','):
        if i != '':
            yield (i,1)

batch_size = int(sys.argv[2])
window_size = int(sys.argv[1])
#print(window_size)
#print(batch_size)
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,batch_size)
#ssc.checkpoint("hdfs://localhost:9000/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
dataStream=dataStream.window(window_size,1)
#dataStream=dataStream.decode()
#dataStream.pprint()
#tweet=dataStream.map(tmp)
# OR
#tweet=dataStream.map(lambda w:(w.split(';')[0]),1)
#tweet.pprint()
#count=tweet.reduceByKey(lambda x,y:x+y)
def out(rdd):
    r=rdd.collect()
#    print(len(r))
    if(len(r)!=0):
        print(r[0][0],r[1][0],r[2][0],r[3][0],r[4][0],sep=",")



count=dataStream.map(lambda w:w.split(";")[7])
#hashtags=count.flatMap(lambda w:(w.split(","),1))
hashtags=count.flatMap(lambda w:tmp(w))
#hashtags.pprint()
#count=hashtags.reduceByKey(add)
count=hashtags.reduceByKey(lambda x,y: x+y)
mySort = count.transform(lambda rdd: rdd.sortBy(lambda x: x[1],ascending=False))
#mySort.pprint(5)
mySort = mySort.foreachRDD(out)
#output=mySort.take(5)
#print(output[0],output[1],output[2],output[3],output[4],sep=",")
#TO maintain state
# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()

#To Perform operation on each RDD
# totalcount.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
