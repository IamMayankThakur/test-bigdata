import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys

def tmp(x):
    for i in x.split(','):
        if i != '':
            yield (i,1)

batch_size = int(sys.argv[2])
window_size = int(sys.argv[1])
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,batch_size)
# ssc.checkpoint("~/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
dataStream=dataStream.window(window_size,1)
#dataStream=dataStream.decode()
#dataStream.pprint()
#tweet=dataStream.map(tmp)
# OR
#tweet=dataStream.map(lambda w:(w.split(';')[0]),1)
#tweet.pprint()
#count=tweet.reduceByKey(lambda x,y:x+y)
def out(rdd):

    rinterm=rdd.sortBy(lambda x: (-x[1],x[0]))
    r=rinterm.collect()
#    print(len(r))
    if(len(r)!=0):
        print(r[0][0],r[1][0],r[2][0],r[3][0],r[4][0],sep=",")


count=dataStream.map(lambda w:w.split(";")[7])

hashtags=count.flatMap(lambda w:tmp(w))
count=hashtags.reduceByKey(lambda x,y: x+y)

# mySort = count.transform(lambda rdd: rdd.sortBy(lambda x: (-x[1],x[0])))
#mySort.pprint()
mySort = count.foreachRDD(out)
ssc.start()
ssc.awaitTermination(60)
ssc.stop()
