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
def processRdd(rdd):
	top5=rdd.take(5)
	if len(top5)>0:
		top4=top5[:4]
		for hashtag in top4:
			print(hashtag[0],end=",")
		print(top5[-1][0])
window_size=int(sys.argv[1])
batch_size=int(sys.argv[2])

conf=SparkConf()
conf.setAppName("Task2")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,batch_size) #batch size
ssc.checkpoint("/checkpoint_BIGDATA")

if len(sys.argv) != 3:
        print("Usage ./submit-spark <file to python file> <window_size> <batch_size>", file=sys.stderr)
        sys.exit(-1)


dStream=ssc.socketTextStream("localhost",9009)
indexofHashtags=7
#tweet=dataStream.window(window_size,1).flatmap(lambda w:w.split(';')[indexofHashtags].split(',')).map(lambda y:(y,1)).filter(lambda z:z[0] is not '')
tweet=dStream.window(window_size,1).map(lambda w:w.split(';')[indexofHashtags].split(','))\
							.flatMap(lambda x:x)
count=tweet.map(lambda hashtag:(hashtag,1))
filteredHashtags=count.filter(lambda hashtag:hashtag[0] is not '')
# allHashtags=dataStream.window(window_size,1).map(lambda w:w.split(';')[indexofHashtags].split(',')).flatmap(lambda x:x)
# #allHashtags=allHashtags.flatmap(lambda x:x) #split all hashtags based on

# HashTags=allHashtags.filter(lambda hashtag:isNotNull(hashtag))
# HashtagCount=HashTags.map(lambda y:(y,1))
# Hah

HashTagsPopular=filteredHashtags.reduceByKey(lambda x,y:x+y)
sorted_count = HashTagsPopular\
			.transform(lambda rdd: rdd.sortBy(lambda x:(-x[1],x[0])))\
			.foreachRDD(processRdd)
ssc.start()
ssc.awaitTermination(22)
ssc.stop()
