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

def process_rdd(time, rdd):
		
		try:
			sql_context = get_sql_context_instance(rdd.context)
			rdd = rdd.filter(lambda w:w[0]!="")			
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tweetid,no_of_tweets from hashtags order by no_of_tweets desc limit 5")
			
			top_tags = ((str(x[0]),x[1]) for x in hashtag_counts_df.select('*').collect())
			sort=sorted(top_tags,key=lambda x:(-x[1],x[0]))			
			k=0
			for i in sort:
				k+=1
				if(k<5):				
					print(i[0],end=",")
				else:
					print(i[0])
					k=0
			
		except:
			e = sys.exc_info()[0]
			

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
	hashtag=tweet.map(lambda w:(w,1))
	
	count=hashtag.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,int(sys.argv[1]))
	
	count.foreachRDD(process_rdd)
	
	ssc.start()
	ssc.awaitTermination(25)
	ssc.stop()
