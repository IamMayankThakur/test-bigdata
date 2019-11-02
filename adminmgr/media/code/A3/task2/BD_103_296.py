import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys

#from pyspark.sql import SparkSession, functions as fs

def ret_tags(x):
        return x.split(',')

def print_top_5(iter):
    array = []
    for element in iter:
        array.append(element[0])
    print(','.join(array[:5]))

window_size = int(sys.argv[1])
batch_size = int(sys.argv[2])

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
#sc.setLogLevel("ERROR")

ssc=StreamingContext(sc, batch_size)
ssc.checkpoint("./checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
hashtags = dataStream.map(lambda x: ret_tags(x.split(';')[7]))
tags = hashtags.map(lambda x: (x[0], 1) if x[0] else (x[0], 0))
tagcounts = tags.reduceByKeyAndWindow(lambda x, y: x+y, lambda x, y: x-y, window_size, 1)
sorted_tagcounts = tagcounts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending = False))

sorted_tagcounts.foreachRDD(lambda rdd: print_top_5(rdd.collect()))

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
