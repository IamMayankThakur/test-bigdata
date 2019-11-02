import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys

def tmp(x):
    for i in x.split(','):
        if i != '':
            yield (i,1)


def out(rdd):
    r=rdd.collect()
    if(len(r)!=0):
        print(r[0][0],r[1][0],r[2][0],r[3][0],r[4][0],sep=",")


batch_size = int(sys.argv[2])
window_size = int(sys.argv[1])

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,batch_size)

dataStream=ssc.socketTextStream("localhost",9009)
dataStream=dataStream.window(window_size,1)


count=dataStream.map(lambda w:w.split(";")[7])

hashtags=count.flatMap(lambda w:tmp(w))
count=hashtags.reduceByKey(lambda x,y: int(x)+int(y))
mySort = count.transform(lambda rdd: rdd.sortBy(lambda x: (x[1],x[0]),ascending=False))
mySort = mySort.foreachRDD(out)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
