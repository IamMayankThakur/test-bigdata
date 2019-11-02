# import findspark
# findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
	try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tweetid,no_of_tweets from hashtags order by no_of_tweets desc,tweetid asc limit 5")
			final=hashtag_counts_df.rdd
			final1 = final.map(lambda x:x.tweetid)
			#print(final1.collect())
			for i in range(0,4):
				print(final1.collect()[i],end=",")
			print(final1.collect()[4])
			
	except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

a=int(sys.argv[1])
b=int(sys.argv[2])

ssc=StreamingContext(sc,1)
ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(lambda w:w.split(';')[7])
tweet1=tweet.flatMap(lambda w:(w.split(',')))
tweet2=tweet1.map(lambda w:(w,1))

count=tweet2.reduceByKeyAndWindow(lambda x,y:x+y , lambda x,y:x-y,a,b).filter(lambda x:x[0] is not '')
count.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
