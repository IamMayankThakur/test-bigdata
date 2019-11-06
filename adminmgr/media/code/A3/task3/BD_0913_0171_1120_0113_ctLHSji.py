import findspark
findspark.init()
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext

import sys
import requests
def cr(l):
    if "," not in l.split(";")[7]:
        return [l.split(";")[7]]
    return l.split(";")[7].split(",")
def f(aw):
    aw1 = aw.sortBy(lambda x: (-x[1],x[0]))
    r = aw1.collect()
    if(r!=[]):
        f1(r)
def f1(inp):
    count=0
    j=0
    while(count!=5):
        if(inp[j][0]!=""):
            if(count!=4):
                print(inp[j][0],end=",")
            else:
                print(inp[j][0])
            count+=1
        j = j+1

configuration=SparkConf()
configuration.setAppName("BigData")
spark_context=SparkContext(configuration=configuration)
stream_context=StreamingContext(spark_context,int(sys.argv[2]))
stream_context.checkpoint("~/checkpoint_BIGDATA")
stream=stream_context.socketTextStream("localhost",8000)
o=dataStream.window(int(sys.argv[1]),1).flatMap(cr).map(lambda z : (z, 1)).reduceByKey(lambda a,b : int(a)+int(b))
o.foreachRDD(f)
stream_context.start()
stream_context.awaitTermination(60)
stream_context.stop()
