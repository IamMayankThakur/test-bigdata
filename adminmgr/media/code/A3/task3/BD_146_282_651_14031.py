import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def parseData(x):
    parts = x.split(";")[7]
    parts1=parts.split(",")
    for i in parts1:
        return i,1
def sortby(rdd):
	ranks=rdd.sortBy(lambda x: x[0])
	final=ranks.sortBy(lambda x: x[1],ascending=False)
	return final

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark streaming <file> <batch_size> <window_size>", file=sys.stderr)
        sys.exit(-1)
window_size = int(sys.argv[1])
batch_size = int(sys.argv[2])

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,batch_size)
ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
parse_data = dataStream.map(lambda x:parseData(x))
grouped=parse_data.reduceByKeyAndWindow(lambda x,y:x+y, lambda x,y:x-y , window_size, 1)
ranksSorted1=grouped.transform(sortby)
#final=ranksSorted1.sortBy(lambda x: x[1],ascending=False)
#tweet=tweet.map(lambda w:("Tweets in android",1))
#totalcount=tweet.updateStateByKey(aggregate_tweets_count)
grouped.pprint()
#parse_data.pprint()
ssc.start()
ssc.awaitTermination(12)
ssc.stop()
