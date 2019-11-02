from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def splitH(tweets):
        for i in tweets:
                yield (i,1)
def disp(x):
        j=0
		l=[]
        o=x.collect()
        #o=sorted(o,key=lambda y: -y[1])
        #o=x.sortBy(lambda a: (-a[1],a[0])).collect()
        for h,c in o:
                if j<5 and o!="":
                      #print(h,end=",")
					  l.append((h,c))
                if j>5:
                      break
                j+=1
        for x in range(0,5):
		        for y in range(0,x):
				      if(l[y+1][1]==l[y][1]):
					    if(l[y][0]>l[y+1][0]):
						    l[y],l[y+1]=l[y+1],l[y]
		
        for h,c in l:
                if j<4 and l!="":
                      print(h,end=",")
					  #l.append((h,c))
                if j == 4:
                      print(h)
					  #l.append((h,c))
                if j>5:
                      break 
                j+=1					  
conf=SparkConf()
conf.setAppName("BigDataAssignent3Task2")
sc=SparkContext(conf=conf)
ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("~/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)

tweets=dataStream.flatMap(lambda x: splitH(x.split(';')[7].split(','))).filter(lambda y: len(y[0])>1)
        
tweetsCount=tweets.reduceByKeyAndWindow(lambda x,y:x+y,None,int(sys.argv[1]),1)
        #reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y :x-y,int(sys.argv[1]),1)
        
finaltweets=tweetsCount.map(lambda x: (x[1],x[0])).transform(lambda rdd: rdd.sortByKey(False)).map(lambda y:(y[1],y[0]))
        
finaltweets.foreachRDD(lambda x: disp(x))

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
