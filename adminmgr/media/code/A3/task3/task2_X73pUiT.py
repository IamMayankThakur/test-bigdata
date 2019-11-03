import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


def sorted_print(rdd):
    sort_rdd = rdd.sortBy(lambda k: (-k[1],k[0])).filter(lambda x : x[0] != '')
    s_list = sort_rdd.collect()
    if(s_list != []):
        print(s_list[0][0],s_list[1][0],s_list[2][0],s_list[3][0],s_list[4][0],sep=",")


def hastags_func(line):
    n = line.split(";")[7]
    if(',' in n):
        return n.split(",")
    return [n]

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
hash_count=dataStream.window(int(sys.argv[1]),1)\
                .flatMap(hastags)\
                .map(lambda hashtag : (hashtag, 1))\
                .reduceByKey(lambda x,y:int(x)+int(y))

hash_count.foreachRDD(sorted_print)

ssc.start()
ssc.awaitTermination(60)
ssc.stop()
