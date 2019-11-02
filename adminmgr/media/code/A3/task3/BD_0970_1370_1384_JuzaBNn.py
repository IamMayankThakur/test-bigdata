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
            
            if(len(r1)>2):
                print("%s,%s,%s,%s,%s" %(r1[0][0],r1[1][0],r1[2][0],r1[3][0],r1[4][0]))
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

if __name__ == "__main__":

    conf=SparkConf()
    conf.setAppName("BigData")
    sc=SparkContext(conf=conf)

    ssc=StreamingContext(sc,int(sys.argv[2]))
    ssc.checkpoint("~/checkpoint_BIGDATA")  

    dataStream=ssc.socketTextStream("localhost",9009)  
    
    tweet=dataStream.map(lambda w:(w.split(';')[7]))
    
    hashtag = tweet.flatMap(lambda w :compute(w))
    
    h = hashtag.window(int(sys.argv[1]),1)
    h1 = h.reduceByKey(lambda x,y:x+y)
    
    h1.foreachRDD(process_rdd)
    
    ssc.start()
    ssc.awaitTermination(60)
    ssc.stop()




