import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def hastags(line):
    k = line.split(";")[7]
    if(',' in k):
        return k.split(",")
    return [k]
def sortandprint(rdd):
    sorted_rdd = rdd.sortBy(lambda a: (-a[1],a[0])).filter(lambda x : x[0] != '')
    sorted_list = sorted_rdd.collect()
    if(sorted_list != []):
        print(sorted_list[0][0],sorted_list[1][0],sorted_list[2][0],sorted_list[3][0],sorted_list[4][0],sep=",")

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("~/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
"""
hashtags_count=dataStream.flatMap(hastags)\
                .map(lambda hashtag : (hashtag, 1))\
                .reduceByKeyAndWindow(lambda x,y:int(x)+int(y),int(sys.argv[1]),1)
"""
hashtags_count=dataStream.window(int(sys.argv[1]),1)\
                .flatMap(hastags)\
                .map(lambda hashtag : (hashtag, 1))\
                .reduceByKey(lambda x,y:int(x)+int(y))

hashtags_count.foreachRDD(sortandprint)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
