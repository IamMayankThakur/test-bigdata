from __future__ import print_function

import findspark
findspark.init()
import sys
from operator import add
from pyspark.streaming import StreamingContext
from pyspark import SparkContext, SparkConf



def splitLines(linesOfRecords):
    hashtags = linesOfRecords.split(";")[7]
    if(',' in hashtags):
        return hashtags.split(",")
    return [hashtags]


def sortrecord(rdd):
    rddIntermediate = rdd.sortBy(lambda a: (-a[1],a[0])).filter(lambda x : x[0] != '') #descending sort
    sortedIterable = rddIntermediate.collect()
    if(sortedIterable != []):
        print(sortedIterable[0][0],sortedIterable[1][0],sortedIterable[2][0],sortedIterable[3][0],sortedIterable[4][0],sep=",")

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: task2.py <Windows size> <batch duration>", file=sys.stderr)
        sys.exit(-1)

    windowSize = int(sys.argv[1])
    batch = int(sys.argv[2])

    configuration = SparkConf()
    configuration.setAppName("ThreeMostCommonHashtags")
    sc=SparkContext(conf=configuration)

    #sc = SparkContext(appName="ThreeMostCommonHashtags")
    ssc = StreamingContext(sc, batch)

    lines = ssc.socketTextStream('localhost', 9009)

    counts = lines.window(windowSize,1).flatMap(splitLines).map(lambda x : (x,1)).reduceByKey(add)

    counts.foreachRDD(sortrecord)
    #counts.pprint()
    ssc.start()
    ssc.awaitTermination(25)
    ssc.stop()
