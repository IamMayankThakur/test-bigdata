import findspark
findspark.init()
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
configuration=SparkConf()
configuration.setAppName("BigData")
def rc(line):

    t=line.split(";")[7]
    if ',' not in t:
        return [t]
    return t.split(",")
def f1(r):
    sr = r.sortBy(lambda x: (-x[1],x[0]))
    srr = sr.collect()
    c=0
    i=0
    if(srr!=[]):
        while(c!=5):
            if(srr[i][0]!=''):
                if(c!=4):
                    print(srr[i][0],end=',')
                else:
                    print(srr[i][0])
                c+=1
            i+=1
context=SparkContext(conf=conf)
s1=StreamingContext(context,int(sys.argv[2]))
s1.checkpoint("~/checkpoint_BIGDATA")
dataStream=s1.socketTextStream("localhost",9009)
finalans=dataStream.window(int(sys.argv[1]),1).flatMap(rc).map(lambda nam : (nam, 1)).reduceByKey(lambda a,b:int(a)+int(b))
finalans.foreachRDD(f1)
s1.start()
s1.awaitTermination(60)
s1.stop()
