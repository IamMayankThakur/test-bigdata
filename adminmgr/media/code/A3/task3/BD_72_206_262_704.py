import findspark

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)
    
def get_sql_context_instance(spark_context):
    if('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']
    
def process_rdd(time, rdd):
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(tag=w[0], count=w[1]))
        hashtags_df = sql_context.createDataFrame(row_rdd, samplingRatio=1)
        hashtags_df.registerTempTable("result")
        hashtag_counts_df = sql_context.sql("select tag, count from result order by count desc limit 5")
        top_tags = [str(t.tag) for t in hashtag_counts_df.select("tag").collect()]
        x =""
        for i in top_tags:
            if (i != ''):
                x+=i
                x+=','
        print(x[:-1])
        x=""
    except:
       e = sys.exc_info()[0]
   

window_size = int(sys.argv[1])
batch_size = int(sys.argv[2])

conf = SparkConf()
conf.setAppName("FifaApp")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, batch_size)
ssc.checkpoint("/usr/local/hadoop/checkpoint_FifaApp")
dataStream = ssc.socketTextStream("localhost",9009)

#tweet = dataStream.window(window_size, 1)
#tweet = dataStream.map(lambda w : w.split(';')[7])

tweet = dataStream.map(lambda w : w.split(';')[7])
tweet = tweet.flatMap(lambda x: x.split(','))
tweet = tweet.map(lambda y : (y,1))
#tagsTotal = tweet.reduceByKey(aggregate_tags_count)
#tagsTotal = tweet.reduceByKeyAndWindow(aggregate_tags_count,None,window_size,1)
tagsTotal = tweet.updateStateByKey(aggregate_tags_count)
tagsTotal.foreachRDD(process_rdd)
#tagsTotal.pprint()




ssc.start()

ssc.awaitTermination(25)

ssc.stop()
