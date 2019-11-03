import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


def parseData(x):
    parts = x.split(";")[7].split(",")
    return list(parts)
    

def printfunc(rdd):
    rdd1=rdd.sortBy(lambda x:(-x[1],x[0])).filter(lambda y: y[0] != '')
    list1=rdd1.collect()
    if(list1!=[]):
    	print(list1[0][0],list1[1][0],list1[2][0],list1[3][0],list1[4][0],sep=",")
    '''
    cnt=1
    for i in rdd.collect():
        if(i[0] != '' and cnt < 5):        
            print(i[0] + ",", end='')
            cnt = cnt + 1
        elif(i[0] != ''):
            print(i[0])
            cnt = 0
            break
    '''
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark streaming <file> <window_size> <batch_size>", file=sys.stderr)
        sys.exit(-1)
window_size = int(sys.argv[1])
batch_size = int(sys.argv[2])

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,batch_size)
ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
h_counts=dataStream.window(window_size,1)\
            .flatMap(parseData)\
            .map(lambda x: (x,1))\
            .reduceByKey(lambda x,y:int(x)+int(y))
#parse_data = dataStream.map(lambda x:parseData(x))
#grouped=parse_data.reduceByKeyAndWindow(lambda x,y:x+y, lambda x,y:x-y , window_size, batch_size)
#grouped=parse_data.reduceByKeyAndWindow(lambda x,y:x+y, window_size, batch_size)
#sorted_1=grouped.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[0]))
h_counts.foreachRDD(printfunc)

ssc.start()
ssc.awaitTermination(60)
ssc.stop()


