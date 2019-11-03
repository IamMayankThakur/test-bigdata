from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


def get_top_keywords(tweets, window_length, sliding_interval):

    addFunc = lambda x, y: x + y
    invAddFunc = lambda x, y: x - y
   
    topics_counts = tweets.reduceByKeyAndWindow(addFunc,invAddFunc,window_length, sliding_interval)
    #topics_counts = tweets.reduceByKeyAndWindow(addFunc,window_length, sliding_interval)
    
    #ranks=ranks.takeOrdered(len(ranks.collect()),key=lambda x: (-x[1],x[0]))
    
    #sorted_topics = topics_counts.transform(lambda rd: rd.sortBy(lambda x:x[1],ascending = False))
    #sorted_t = sorted_topics.transform(lambda rd: rd.takeOrdered(5,key=lambda x: x[0]))
    #top=sorted_topics.transform(lambda x:x[0])
    
    sorted_t=topics_counts.transform(lambda rd: rd.sortBy(lambda x:(-x[1],x[0])))
    #sorted_t.pprint(5)

    return sorted_t

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
#tweet=dataStream.map(lambda w:(w.split(';')[7],1))
tweet=dataStream.map(lambda w:w.split(';')[7])
#tweet1=tweet.flatMap(lambda w:list(map(lambda x:(x,1),w[0].split(','))))
tweet1=tweet.flatMap(lambda w:w.split(','))
tweet1=tweet1.map(lambda w:(w,1))
tweets=tweet1.filter(lambda x:x[0]!='')
top_topics = get_top_keywords(tweets,window_length, sliding_interval)
def pr(rd):
    if(len(rd.collect())==0):
        #print()
        return
    for i in range(4):
        print(rd.collect()[i][0],end=",")
    print(rd.collect()[4][0])
    #print()
top_topics.foreachRDD(lambda x:pr(x))
#h_list=top_topics.values.tolist()
#print(h_list[0][0],h_list[0][1],h_list[0][2],h_list[0][3],h_list[0][4],sep=",")

#top_topics.pprint(5)

ssc.start()
ssc.awaitTermination(60)
ssc.stop()



