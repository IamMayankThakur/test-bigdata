
import sys
import requests
import findspark
findspark.init()

from pyspark.sql import Row,SQLContext
from pyspark.streaming import StreamingContext
from pyspark import SparkConf,SparkContext


def sort_print(rdd):
    rdd1 = rdd.sortBy(lambda line: (-line[1],line[0])).filter(lambda x : x[0] != '')
    s_list = rdd1.collect()
    if(s_list != []):
        print(s_list[0][0],s_list[1][0],s_list[2][0],s_list[3][0],s_list[4][0],sep=",")

def hastags(line):
    h_tag = line.split(";")[7]
    if(',' in h_tag):
        return h_tag.split(",")
    return [h_tag]


conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("~/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
count=dataStream.window(int(sys.argv[1]),1).flatMap(hastags).map(lambda hashtag : (hashtag, 1)).reduceByKey(lambda x,y:int(x)+int(y))

count.foreachRDD(sort_print)

ssc.start()
ssc.awaitTermination(60)
ssc.stop()
