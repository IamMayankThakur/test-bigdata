# import findspark
# findspark.init()
from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from operator import add

import sys
import requests

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window


if __name__ == "__main__":

    window_size, batch_size = int(sys.argv[1]), int(sys.argv[2])
    conf = SparkConf()
    conf.setAppName("BigData")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, int(batch_size))
    ssc.checkpoint("/home/kishan/Downloads/chkpt ")
    lines = ssc.socketTextStream("localhost", 9009)
    # lines = lines.window(int(window_size),1)
    # lines.pprint()
    words = lines.map(lambda line: line.split(";")[7])
    # words.pprint()
    words = words.flatMap(lambda x: x.split(","))
    # words.pprint()
            
    hashtag = words.map(lambda x: (x,1))
    # hashtag.pprint()
    
    #hashtag = hashtag.rdd
    
    windowedWordCounts = hashtag.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,int(window_size),1)
    # windowedWordCounts.pprint()
    # h = hashtag.groupBy(lambda x: x[0]).map(lambda y: y[1].reduce(add))
    # hashtag.pprint()
    # h.pprint()
    #windowedWordCounts = hashtag.reduceByKey(add)
    
    # h = hashtag.countByWindow(int(window_size),1)
    # h.pprint()
    # windowedWordCounts = hashtag.countByValueAndWindow(int(window_size),1)
    # windowedWordCounts = hashtag.countByWindow(int(window_size),1)

    windowedWordCounts.pprint()
    
    

    ssc.start()
    ssc.awaitTermination(25)
    ssc.stop()

