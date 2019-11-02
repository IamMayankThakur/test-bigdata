from __future__ import print_function

import sys

import time

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql import Row,SQLContext

def process_rdd(time, rdd):
		it=0
		for (i,j) in rdd.collect():
		   if it==4:
		      print(i)
		      break
		   else:
		      print(i,end=',')
		   it+=1
def somefunc(someval):
    x=someval.split(',')
    for y in x:
        if y != '':
            yield (y,1)
      
               

if __name__ == "__main__":
    if len(sys.argv) != 3:
        msg = ("Usage: structured_network_wordcount_windowed.py "
               "<window duration in seconds> <batch>")
        print(msg, file=sys.stderr)
        sys.exit(-1)

    windowSize = int(sys.argv[1])
    batchDuration = int(sys.argv[2])
    slideSize=1
    conf=SparkConf()
    conf.setAppName("BigData")
    sc=SparkContext(conf=conf)

    ssc=StreamingContext(sc,1)
    ssc.checkpoint("/home/hadoop/checkpoint_BIGDATA")

    dataStream=ssc.socketTextStream("localhost",9009)
    #print(type(dataStream))
    #dataStream.pprint()	
    #print(windowSize)
    windowstream=dataStream.window(windowSize,batchDuration)
    
    hashtags=windowstream.map(lambda x:(x.split(';')[7]))
    #hashtags.pprint()
    hashes=hashtags.flatMap(lambda x:somefunc(x))
    #hashes.pprint()
    
    tags=hashes.reduceByKey(lambda x,y: x+y)

    alpha=tags.transform(lambda x:x.sortBy(lambda y:(y[0])))
    srtd=alpha.transform(lambda x:x.sortBy(lambda y:(-y[1])))

    srtd.foreachRDD(process_rdd) 

    
    ssc.start()
    ssc.awaitTermination(25)
    ssc.stop()

