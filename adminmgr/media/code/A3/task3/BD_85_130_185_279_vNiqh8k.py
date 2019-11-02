import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

if __name__ == "__main__":
	#print(sys.argv[0],sys.argv[1],sys.argv[2])
	if len(sys.argv)!=3:
		sys.exit(-1)


ssc=StreamingContext(sc,2)
ssc.checkpoint("~/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
dataStream.pprint()
words = dataStream.flatMap(lambda line: line.split(";")[7].split(","))
pairs = words.map(lambda word: (word, 1))
windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, int(sys.argv[1]), int(sys.argv[2]))
windowedWordCounts.pprint()

ssc.start()
ssc.awaitTermination(12)
ssc.stop()
