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
'''
def printing(rdd):

    sort1 = rdd.collect()
    r=sort1.sort(key=lambda a: (-a[1],a[0]))
    print(r)
  

'''
'''
def printing(rdd):
    sort1 = rdd.sortBy(lambda x: (-x[1],x[0]))
    r=sort1.collect()
    if(len(r)<5):
        return
    c=0
    i=0
    while(c!=5):
     if(r[i][0]!=''):
        if(c==4):
            print(r[i][0])
        else:
            print(r[i][0],end=',')
        c=c+1
     i=i+1

'''

def printing(rdd):
    s1 = rdd.sortBy(lambda x: (-x[1],x[0]))
    sort2 = s1.collect()
    count=0
    i=0
    s=list()
    if(sort2!=[]):
        while(count!=5):

            if(sort2[i][0]!=''):
                    s.append(sort2[i][0])
                    count+=1
            i=i+1
        print(s[0],s[1],s[2],s[3],s[4],sep=',')



if __name__ == "__main__":
	conf=SparkConf()
	conf.setAppName("BigData")
	sc=SparkContext(conf=conf)

	ssc=StreamingContext(sc,int(sys.argv[2]))
	ssc.checkpoint("~/checkpoint_BIGDATA")

	dataStream=ssc.socketTextStream("localhost",9009)
	t=dataStream.window(int(sys.argv[1]),1)
	tweet=t.map(lambda w: w.split(';')[7])
	tweet1=tweet.flatMap(lambda w:w.split(','))
	tweet2=tweet1.map(lambda w:(w,1))
	tweet3=tweet2.reduceByKey(lambda x,y:x+y)

	tweet3.foreachRDD(printing)

	ssc.start()
	ssc.awaitTermination(60)
	ssc.stop()
