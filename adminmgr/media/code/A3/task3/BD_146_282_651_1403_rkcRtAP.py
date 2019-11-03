import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


def parseData(x):
    parts = x.split(";")[7]
    parts1=parts.split(",")
    for i in parts1:
        return i,1
def sortby(rdd):
	ranks=rdd.sortBy(lambda x: x[0])
	final=ranks.sortBy(lambda x: x[1],ascending=False)
	return final

def printfunc(rdd):
	cnt=1
	for i in rdd.collect():
		if(i[0] != '' and cnt < 5):		
			print(i[0] + ",", end='')
			cnt = cnt + 1
		elif(i[0] != ''):
			print(i[0])
			cnt = 0
			break;
	
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark streaming <file> <window_size> <batch_size>", file=sys.stderr)
        sys.exit(-1)
window_size = int(sys.argv[1])
batch_size = int(sys.argv[2])

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,1)
ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
parse_data = dataStream.map(lambda x:parseData(x))
#grouped=parse_data.reduceByKeyAndWindow(lambda x,y:x+y, lambda x,y:x-y , window_size, batch_size)
grouped=parse_data.reduceByKeyAndWindow(lambda x,y:x+y, window_size, 1)
sorted_1=grouped.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[0]))
sorted_2=sorted_1.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
final=sorted_2.foreachRDD(printfunc)

ssc.start()
ssc.awaitTermination(60)
ssc.stop()
