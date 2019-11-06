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
def fab1(r):
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
sc=SparkContext(conf=conf)
output=StreamingContext(sc,int(sys.argv[2]))
output.checkpoint("~/checkpoint_BIGDATA")
dataStream=output.socketTextStream("localhost",9009)
x1=dataStream.window(int(sys.argv[1]),1)
x2= x1.flatMap(rc).map(lambda ab : (ab, 1))
x3 = x2.reduceByKey(lambda a,b:int(a)+int(b))
x3.foreachRDD(fab1)
output.start()
output.awaitTermination(25)
output.stop()
