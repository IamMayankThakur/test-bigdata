import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests




def removecomma(line):

    t=line.split(";")[7]
    if ',' not in t:
        return [t]
    else:
        y=t.split(",")
        return y


def sortans(r):
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
        #print()






conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("~/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)



finalans=dataStream.window(int(sys.argv[1]),1).flatMap(removecomma).map(lambda apporve : (apporve, 1)).reduceByKey(lambda x,y:int(x)+int(y))



finalans.foreachRDD(sortans)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
