import findspark
#findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import *
import requests
from pyspark.sql.functions import *
import sys


def prin(stream):
 
        def takeAndPrint(time, rdd):
            
            i=0
            l=0
            taken = rdd.take(6)
            
            for record in taken:
            	if(record[0] == ''):
            		continue
            	else:
            		if(i<5):
	            		print(record[0],end="")
	            		l+=1
	            		i+=1

            	if(l<5):
                	print(",", end="")

            print("")
			
        stream.foreachRDD(takeAndPrint)


def compute(rdd):
    for x in rdd:
        yield (x, 1)


bat_int= float(sys.argv[2])
wind= float(sys.argv[1])

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,bat_int)


dataStream=ssc.socketTextStream("localhost",9009)

hashtag=dataStream.map(lambda w: w.split(';')[7].strip().split(',')) 

hashtag= hashtag.transform(lambda x: x.flatMap(lambda x:compute(x)))

new_tag= hashtag.window(wind,1)

count=new_tag.reduceByKey(lambda x,y:x+y)
count_sorted= count.transform(lambda rdd: rdd.sortBy(lambda x:(-x[1], x[0])))

prin(count_sorted)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
