import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys

def parse(x):
    for element in x:
        yield element

def ret_tags(x):
        return x.split(',')

def print_top_5(iter):
    # if not(iter):
    #     print('\n')
        # return 
    array = []
    count = 0
    for element in iter:
        if count == 5:
            print(','.join(array))
            return
        else:
            array.append(element[0])
            count += 1

window_size = int(sys.argv[1])
batch_size = int(sys.argv[2])

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
#sc.setLogLevel('ERROR')

ssc=StreamingContext(sc, 1)
ssc.checkpoint("./checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
hashtags = dataStream.map(lambda x: ret_tags(''.join(x.split(';')[7]))).window(window_size, batch_size).flatMap(lambda x: parse(x)).filter(lambda x: not(x == '')).map(lambda x: (x, 1))
tagcounts = hashtags.reduceByKey(lambda x, y: x+y)
sorted_tagcounts = tagcounts.transform(lambda rdd: rdd.sortByKey()).transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

sorted_tagcounts.foreachRDD(lambda rdd: print_top_5(rdd.collect()))
ssc.start()
ssc.awaitTermination(100)
ssc.stop()
