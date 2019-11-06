import findspark
findspark.init()
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
def rc(line):

    t=line.split(";")[7]
    if ',' not in t:
        return [t]
    else:
        y=t.split(",")
        return y
def fab(r):
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
conf=SparkConf()
conf.setAppName("BigData")
ab=SparkContext(conf=conf)
cc=StreamingContext(ab,int(sys.argv[2]))
cc.checkpoint("~/checkpoint_BIGDATA")
stream=cc.socketTextStream("localhost",9009)
finalans=stream.window(int(sys.argv[1]),1).flatMap(rc).map(lambda x : (x, 1)).reduceByKey(lambda a,b:int(a)+int(b))
finalans.foreachRDD(fab)
cc.start()
cc.awaitTermination(25)
cc.stop()
