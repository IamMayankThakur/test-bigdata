import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys

def myprint(rdd,num):
    taken = rdd.take(num)
    for record in taken[0:1]:
        print(record[0],end='')
    for record in taken[1:num]:
        print(","+record[0],end='')
    if len(taken) != 0:
        print()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Task_2.py <window_size> <batch_duration>", file=sys.stderr)
        sys.exit(-1)

    window_duration = int(sys.argv[1])
    batch_duration = int(sys.argv[2])

    conf=SparkConf()
    conf.setAppName("BigData_Assignment3")
    sc=SparkContext(conf=conf)

    ssc=StreamingContext(sc,batch_duration)
    ssc.checkpoint("/checkpoint_BIGDATA")

    dataStream=ssc.socketTextStream("localhost",9009)

    hashtag = dataStream.map(lambda row: row.split(";")[7]).filter(lambda w: len(w) > 0)
    hashtags = hashtag.flatMap(lambda line: line.split(",")).map(lambda word: (word,1))
    windowedHashtagCounts = hashtags.reduceByKeyAndWindow(lambda x, y: x + y,lambda x, y: x - y, window_duration,1)

    top = windowedHashtagCounts.transform(lambda rdd: rdd.sortBy(lambda x: (-x[1],x[0])))

    # top.pprint(5)
    top.foreachRDD(lambda rdd : myprint(rdd,5))
    ssc.start()
    ssc.awaitTermination(25)
    ssc.stop()