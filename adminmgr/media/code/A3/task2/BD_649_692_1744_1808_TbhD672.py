
import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def process_rdd(rdd):  
    sort = rdd.sortBy(lambda x: (-x[1],x[0])).filter(lambda y: y[0] !='').collect()
    if(sort!=[]):
    #print(sorted_list[:5])
        print(sort[0][0],sort[1][0],sort[2][0],sort[3][0],sort[4][0],sep=",")


def out(l):
  o=l.split(";")[7]
  if(',' not in o):
    return [o]
  return o.split(",")
  

conf=SparkConf()
conf.setAppName("BD3")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("~/checkpoint_BIGDATA2")
dataStream=ssc.socketTextStream("localhost",9009)

hashtags=dataStream.window(int(sys.argv[1]),1)
all_hashtags=hashtags.flatMap(out)
result=all_hashtags.map(lambda h : (h,1))
final_result=result.reduceByKey(lambda x,y:int(x)+int(y))
final_result.foreachRDD(process_rdd)
ssc.start()
ssc.awaitTermination(25)
ssc.stop()
