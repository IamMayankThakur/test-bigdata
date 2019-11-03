import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from operator import add

def process_rdd(time, rdd):
		try:
			rdd = rdd.filter(lambda x: x[0]!='')
			row_rdd = rdd.map(lambda w: Row(hashtag=w[0],hashtag_count=w[1]))
			top_hashtags = row_rdd.collect()
			result = sorted(top_hashtags,key=lambda x:(-x[1],x[0]))
			if(len(result) > 4):
				print(result[0][0]+','+result[1][0]+','+result[2][0]+','+result[3][0]+','+result[4][0])
		except:
			e = sys.exc_info()[0]


conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
windowSize = int(sys.argv[1])
batchSize = int(sys.argv[2])
ssc=StreamingContext(sc,batchSize)
ssc.checkpoint("/checkpoint_BIGDATA")
dataStream=ssc.socketTextStream("localhost",9009)
words = dataStream.flatMap(lambda line: ((line.split(";")[7]).split(",")))
hashtags = words.map(lambda x: (x, 1))
tags_totals=hashtags.reduceByKeyAndWindow(lambda m,n:m+n,lambda m,n:m-n,windowSize,1)
tags_totals.foreachRDD(process_rdd)

ssc.start() 
ssc.awaitTermination(25)
ssc.stop()
