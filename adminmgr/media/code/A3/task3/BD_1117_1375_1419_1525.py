from __future__ import print_function
import findspark
findspark.init()


from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, window
from pyspark.sql.types import StructType
from pyspark.sql.functions import desc


def ironMan(x,t):
    t = t.sortBy(lambda x : (-x[1], x[0]))
    a = t.collect()
    m = 5;
    ans = ""
    for i in a:
        m -= 1
        if(i[0]!=" " and i[0]!=""):
            ans = ans + i[0]
        
            if m == 0:
                break
            ans += ','
        else:
            m+=1
    if(len(ans)>1):
        print(ans)



if (len(sys.argv)!=3):
    sys.exit(1)

wolverine = float(sys.argv[1])
deadpool = float(sys.argv[2])





conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,deadpool)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)

tweet = dataStream.window(wolverine,1)

tweet=tweet.flatMap(lambda w:w.split(';')[7].split(','))
tweet = tweet.map(lambda x : (x,1))

count=tweet.reduceByKey(lambda x,y:x+y)
count.foreachRDD(ironMan)



ssc.start()
ssc.awaitTermination(30)
ssc.stop()

