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



def printrdd(rdd):
	Hlist=[]
	i=0
	for hashtag in rdd.collect():
		if(hashtag!=''):
			Hlist.append(hashtag)
			i=i+1
		if(i==5):
			break
	string = ",".join(Hlist)
	print(string)
    #Hlist=HList[0:-1]
    
        



if __name__ == "__main__":

    window_size, batch_size = int(sys.argv[1]), int(sys.argv[2])
    conf = SparkConf()
    conf.setAppName("BigData")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, int(batch_size))
    # ssc.checkpoint("/home/cdiya/Downloads/checkpoints")
    ssc.checkpoint("/checkpoint_BIGDATA")
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
    windowedWordCounts = windowedWordCounts.filter(lambda x: x[0]!='')
    
    topHash = windowedWordCounts.transform(lambda rdd: rdd.sortBy(lambda x:(-x[1],x[0]),ascending=True))
    #topHash = topHash.transform(lambda rdd:))
    topHash = topHash.transform(lambda rdd: rdd.map(lambda x: x[0]))
    topHash.foreachRDD(printrdd)
   
    #topHash.pprint(5)
    

    ssc.start()
    
    ssc.awaitTermination(25)
    ssc.stop()

