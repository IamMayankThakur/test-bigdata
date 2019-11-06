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
    if ',' not in line.split(";")[7]:
        return [line.split(";")[7]]
    return line.split(";")[7].split(",")
def f1(r):
    ab1 = r.sortBy(lambda x: (-x[1],x[0]))
    ab2 = ab1.collect()
    c=0
    i=0
    if(ab2!=[]):
        while(c!=5):
            if(ab2[i][0]!=''):
                if(c!=4):
                    print(ab2[i][0],end=',')
                else:
                    print(ab2[i][0])
                c+=1
            i+=1
    
context=SparkContext(conf=conf)
s1=StreamingContext(context,int(sys.argv[2]))
s1.checkpoint("~/checkpoint_BIGDATA")
stream=s1.socketTextStream("localhost",9009)
#x1=stream.window(int(sys.argv[1]),1)
#x2 = x1.flatMap(rc).map(lambda nam : (nam, 1))
#output=x2.reduceByKey(lambda a,b:int(a)+int(b))
output=dataStream.window(int(sys.argv[1]),1).flatMap(removecomma).map(lambda nam : (nam, 1)).reduceByKey(lambda a,b:int(a)+int(b))
output.foreachRDD(f1)
s1.start()
s1.awaitTermination(25)
s1.stop()
