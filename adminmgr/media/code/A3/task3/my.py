import findspark
findspark.init()
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

win_siz = int(sys.argv[1])
bat_dur = int(sys.argv[2])

def fun(x):
	h = x.split(",")
	for i in h:
        if(len(i)>1):
			yield (i, 1)
		
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,bat_dur)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
twt=dataStream.map(lambda x:x.split(';')[7])
hashtag = twt.flatMap(lambda x:fun(x))
hashtag=hashtag.window(win_siz, 1)


#hashtags=hashtags.filter(lambda x:False if x[0] == "" else True)
#count=hashtags.reduceByKey(lambda x,y:x+y)
count=hashtag.reduceByKeyAndWindow(lambda x, y: x + y,lambda x,y:x-y,win_siz,1)

#count = count.transform(lambda rdd: rdd.sortBy(lambda a: (-a[1], a[0])))

#count = count.transform(lambda rdd: sc.parallelize(rdd.take(3)))

def fun1(rdd):
	r = rdd.collect()
	r.sort(key=lambda a:a[0])
	r.sort(key=lambda a:a[1],reverse=True)
	if(len(r)>4):
		top_5 = r[0:5]
		for i in top_5[0:4]:
			print(i[0],end=",")
		print(top_5[4][0])
    
			
	
count.foreachRDD(fun1)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()