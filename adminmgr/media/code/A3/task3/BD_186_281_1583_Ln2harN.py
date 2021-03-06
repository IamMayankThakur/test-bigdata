
from __future__ import print_function
import findspark
findspark.init()
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def remove_empty_string_if_any(l):
    for i in l:
        if i[0]=='':
            l.remove(i)
            return l
    return l[:-1]

def sortrecord(rdd):
    #l = sorted(rdd.countByKey().items())
    l = rdd.sortBy(lambda x:x[1],ascending=False).collect()[:5]
    #l = remove_empty_string_if_any(l)
    l.sort(key=lambda x:x[0],reverse=False)
    l.sort(key=lambda x:x[1],reverse=True)
    l = list(map(lambda x:x[0],l))
    if l != []:
        print(*l,sep=',')

if len(sys.argv) != 3:
    print("Usage: task2.py <Windows size> <batch duration>", file=sys.stderr)
    sys.exit(-1)
sc = SparkContext(appName="ThreeMostCommonHashtags")
ssc = StreamingContext(sc, int(sys.argv[2]))

lines = ssc.socketTextStream('localhost', 9009)
lines=lines.window(int(sys.argv[1]),slideDuration=1)
counts = lines.map(lambda line: (line.split(';')[7]))
counts = counts.flatMap(lambda x:x.split(','))
counts = counts.map(lambda x:(x,1))
counts = counts.reduceByKey(lambda a, b: a+b)
counts.foreachRDD(sortrecord)
#counts.pprint()

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
