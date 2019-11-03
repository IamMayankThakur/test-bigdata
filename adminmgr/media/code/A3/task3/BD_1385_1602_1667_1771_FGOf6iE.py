import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def display(sort):
	if(len(sort)>4):
		print(sort[0][0]+','+sort[1][0]+','+sort[2][0]+','+sort[3][0]+','+sort[4][0])
def process_rdd(time, rdd):
		
		try:
			sql_context = get_sql_context_instance(rdd.context)
			rdd = rdd.filter(lambda w:w[0]!="")			
			row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
			#hashtags_df = sql_context.createDataFrame(row_rdd)
			#hashtags_df.registerTempTable("hashtags")
			#hashtag_counts_df = sql_context.sql("select tweetid,no_of_tweets from hashtags order by no_of_tweets desc limit 5")
			top_tags=row_rdd.collect()
			sort=sorted(top_tags,key=lambda x:(-x[1],x[0]))			
			k=0
			display(sort)
			
				
			
		except:
			e = sys.exc_info()[0]
			#print("Error",e)

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: <file> <windowsize> <batch_duration>", file=sys.stderr)
		sys.exit(-1)

	conf=SparkConf()
	conf.setAppName("BigData")
	sc=SparkContext(conf=conf)

	ssc=StreamingContext(sc,int(sys.argv[2]))
	ssc.checkpoint("/checkpoint_BIGDATA")
	dataStream=ssc.socketTextStream("localhost",9009)

	tweet=dataStream.flatMap(lambda w:(w.split(';')[7].split(',')))
	hashtags=tweet.map(lambda w:(w,1))
		
	count=hashtags.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,int(sys.argv[1]),1)
		
	count.foreachRDD(process_rdd)
		
	ssc.start()
	ssc.awaitTermination(25)
	ssc.stop()
