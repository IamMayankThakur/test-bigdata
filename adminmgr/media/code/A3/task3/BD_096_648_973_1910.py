from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


def get_top_keywords(tweets, window_length, sliding_interval):

    addFunc = lambda x, y: x + y
    invAddFunc = lambda x, y: x - y
   
    topics_counts = tweets.reduceByKeyAndWindow(addFunc,invAddFunc,window_length, sliding_interval)
    
    #ranks=ranks.takeOrdered(len(ranks.collect()),key=lambda x: (-x[1],x[0]))
    sorted_topics = topics_counts.transform(lambda rd: rd.sortBy(lambda x:x[1],ascending = False))

    return sorted_topics

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
ssc.checkpoint("/home/ziyan/BD")

dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(lambda w:(w.split(';')[7],1))
tweet1=tweet.flatMap(lambda w:list(map(lambda x:(x,1),w[0].split(','))))
tweets=tweet1.filter(lambda x:x[0]!='')
top_topics = get_top_keywords(tweets,window_length, sliding_interval)

top_topics.pprint(3)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
