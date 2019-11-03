from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: try.py <Window size in mins> <Batch duration in sec>", file=sys.stderr)
        sys.exit(-1)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)


batch_interval = int(sys.argv[2])
window_length = int(sys.argv[1])
sliding_interval = 1
    
ssc=StreamingContext(sc,batch_interval)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(lambda w:w.split(';')[7])
tweet1=tweet.flatMap(lambda w:w.split(','))
tweet2=tweet1.map(lambda w:(w,1))
tweets=tweet2.filter(lambda x:x[0]!='')
addFunc = lambda x, y: int(x) + int(y)
invAddFunc = lambda x, y: x - y
   
    
topics_counts1 = tweets.window(window_length, sliding_interval)
topics_counts = topics_counts1.reduceByKey(addFunc)

#topics_counts1 = tweets.reduceByWindow(addFunc,window_length, sliding_interval)
#topics_counts = topics_counts1.reduceByKey(addFunc)

#topics_counts = tweets.reduceByKeyAndWindow(addFunc,window_length, sliding_interval)

    
sorted_topics = topics_counts.transform(lambda rdd: rdd.sortBy(lambda x:(-x[1],x[0])))
#sorted_topics = topics_counts.transform(lambda rd: rd.sortBy(lambda x:x[1],ascending = False))


def pr(rdd):
    if(len(rdd.collect())==0):
        return
    else:
     for i in range(4):
        print(rdd.collect()[i][0],end=",")
     print(rdd.collect()[4][0])
    
sorted_topics.foreachRDD(lambda x:pr(x))

#sorted_topics.pprint(5)

ssc.start()
ssc.awaitTermination(60)
ssc.stop()


