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

    ssc=StreamingContext(sc,batchDuration)
    ssc.checkpoint("/home/hadoop/checkpoint_BIGDATA")

    dataStream=ssc.socketTextStream("localhost",9009)
    #print(type(dataStream))
    #dataStream.pprint()	
    #print(windowSize)
    windowstream=dataStream.window(windowSize,2)
    
    hashtags=windowstream.map(lambda x:(x.split(';')[7]))
    #hashtags.pprint()
    hashes=hashtags.flatMap(lambda x:somefunc(x))
    #hashes.pprint()
    
    tags=hashes.reduceByKey(lambda x,y: x+y)
    #tags.pprint()
    #print(type(tags))
    alpha=tags.transform(lambda x:x.sortBy(lambda y:(y[0])))
    srtd=alpha.transform(lambda x:x.sortBy(lambda y:(-y[1])))
    #print(type(srtd))
    #new=srtd.map(lambda x:print(x,type(x)))
    #srtd=srtd.transform(lambda x:sc.parallelize(rdd.take(3)))
    #srtd.pprint(5)
    srtd.foreachRDD(process_rdd) 

    
    ssc.start()
    ssc.awaitTermination(60)
    ssc.stop()

    #spark = SparkSession\
    #    .builder\
    #    .appName("StructuredNetworkWordCountWindowed")\
    #    .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to host:port
'''    lines = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .option('includeTimestamp', 'true')\
        .load()

    lines.printSchema()
    # Split the lines into words, retaining timestamps
    # split() splits each line into an array, and explode() turns the array into multiple rows
    words = (lines.select(
        split(lines.value, ';')[7],
        lines.timestamp
    )).select(split((lines.select(
        split(lines.value, ';')[7],
        lines.timestamp
    )).value,','))
    words.printSchema()
    
    #tags= words.select(split)
   
    #tags = words.rdd.map(lambda x:(x.split(','),1))
    #print(tags)
    #print(type(words))

    #tags = words.select(''
    #    split(words, ','),
    #    words.timestamp
    #)'''
