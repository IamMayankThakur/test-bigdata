from __future__ import print_function
import re
import sys
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession,SQLContext

def assign(w):
    words = re.split(r',',w)
    for i in words:
        yield (i,1)


def process_rdd(time, rdd):
        try:
            hashtags=sorted(rdd.collect(),key=lambda x:(-x[1],X[0]))
            if(len(hashtags)>4):
                print(hashtags[0][0]+','+hashtags[1][0]+','+hashtags[2][0]+','+hashtags[3][0]+','+hashtags[4][0])
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

if __name__ == "__main__":
    batch_time=int(sys.argv[2])
    window_size=int(sys.argv[1])
    conf=SparkConf()
    conf.setAppName("BigData")
    sc=SparkContext(conf=conf)
    ssc=StreamingContext(sc,1)
    ssc.checkpoint("/checkpoint_BIGDATA")
    # Create a socket stream on target ip:port and count the
    # words in input stream of \n delimited text (eg. generated by 'nc')
    lines1 = ssc.socketTextStream("localhost",9009)
    #tweet2=tweet.filter(lambda w: 'Malaysia' not in w.split(';')[12])
    tweet=lines1.filter(lambda w:w.split(';')[7]!="")
    words1 = tweet.map(lambda line: line.split(';')[7])
    
    words=words1.flatMap(lambda wd : assign(wd))
    #totalcount=words.updateStateByKey(aggregate_tweets_count)
    count=words.reduceByKeyAndWindow(lambda x,y:x+y ,lambda x,y:x-y ,window_size,batch_time)
    #totalcount.pprint()
    #words.pprint()
    
   
    
    count.foreachRDD(process_rdd)
    ssc.start() 
    ssc.awaitTermination(30)
    ssc.stop()
