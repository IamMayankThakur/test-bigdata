import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import time

def myprint(rdd,num):
    taken = rdd.take(num)
    for record in taken[0:1]:
        if record[1] != 0:
            print(record[0],end='')
        else:
            return
    for record in taken[1:num]:
        if record[1] != 0:
            print(","+record[0],end='')
        else:
            break
    if len(taken) != 0:
        print()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        # print("Usage: Task_2.py <window_size> <batch_duration>", file=sys.stderr)
        window_duration = 1
        batch_duration = 1
    else:
        window_duration = float(sys.argv[1])
        batch_duration = float(sys.argv[2])

    conf=SparkConf()
    conf.setAppName("BD_Assignment3_Task2")
    sc=SparkContext(conf=conf)
    
    ssc=StreamingContext(sc,batch_duration)
    ssc.checkpoint("/checkpoint_BIGDATA")

    dataStream=ssc.socketTextStream("localhost",9009)
    # time.sleep(5)
    hashtag = dataStream.map(lambda row: row.split(";")[7]).filter(lambda w: w != '')
    hashtags = hashtag.flatMap(lambda line: line.split(",")).map(lambda word: (word,1))
    windowedHashtagCounts = hashtags.reduceByKeyAndWindow(lambda x, y: x + y,lambda x, y: x - y, window_duration,1)

    top = windowedHashtagCounts.transform(lambda rdd: rdd.sortBy(lambda x: (-x[1],x[0])))

    # top.pprint(5)
    top.foreachRDD(lambda rdd : myprint(rdd,5))

    ssc.start()
    ssc.awaitTermination(25)
    ssc.stop()