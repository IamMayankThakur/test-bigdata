import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def splitHashtags(hashtags):
        for i in hashtags:
                yield (i,1)

def sprint(p):
        if not p.isEmpty():
                out=p.collect()
                j=0
                top5=""
                for i in out:
                        if(j<5):
                                top5=top5+str(i)+","#+str(i[1])+" "
                                j=j+1
                        else:
                                break
                
                top5=top5[:-1]
                print(top5)
                            
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("~/Downloads/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)

hashTags=dataStream.flatMap(lambda x: splitHashtags(x.split(';')[7].split(','))).\
        filter(lambda y: len(y[0])>1)
        
hashTagsCount=hashTags.\
        reduceByKeyAndWindow(lambda agg,obj:agg+obj,None,int(sys.argv[1]),1)

commonHashTags=hashTagsCount.\
        transform(lambda rdd: rdd.sortByKey(True))\
        .map(lambda x: (x[1],x[0]))\
        .transform(lambda rdd: rdd.sortByKey(False))\
        .map(lambda y:(y[1],y[0]))
        
topHashTags=commonHashTags.map(lambda x: x[0])
topHashTags.foreachRDD(sprint)
ssc.start()
ssc.awaitTermination(25)
ssc.stop()
