import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.window import Window
from pyspark.sql import functions as func
import sys
import requests

def xprint(self):
        """
        Print the first num elements of each RDD generated in this DStream.

        @param num: the number of elements from the first will be printed.
        """
        def takeAndPrint(time, rdd):
            taken = rdd.take(5 + 1)
            l=[]
            for record in taken[:6]:
            	if( record[0]!=""):
                	l.append(record[0])
            print (",".join(l[0:5]))

        self.foreachRDD(takeAndPrint)

def split_hashtag(tags):
    for tag in tags:
        yield (tag, 1)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc, float(sys.argv[2]))
ssc.checkpoint("./checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
hashtag=dataStream.map(lambda w:(w.split(';')[7].split(',')))
all_hashtag=hashtag.transform(lambda rdd: rdd.flatMap(lambda w: split_hashtag(w)))
windowSpec=all_hashtag.window(float(sys.argv[1]), 1)
count=windowSpec.reduceByKey(lambda x,y:x+y)
data=count.transform(lambda rdd: rdd.sortBy(lambda x : (-x[1],x[0])))
xprint(data)


ssc.start()
ssc.awaitTermination(25)
ssc.stop()
