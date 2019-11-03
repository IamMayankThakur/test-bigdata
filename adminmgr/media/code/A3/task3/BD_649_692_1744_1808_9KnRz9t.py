import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def process_rdd(rdd):  
    sort = rdd.sortBy(lambda w: (-w[1],w[0])).filter(lambda y: y[0] != '').collect()
    if(sort!=[]):
      #print(sort[:5])
      res_list=[x[0] for x in sort[:5]]
      print(*res_list, sep=",")


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
ssc.awaitTermination(60)
ssc.stop()
