from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession

def computeContribs(batsman, r):
    length = float(len(batsman))
    for i in batsman:
        yield (i, float(float(r) / length))

def splittext(lines):
    parts = re.split(r',+', lines)
    return parts[0], parts[1]

def average(line):
	parts = re.split(r',+', line)
	average = float(float(parts[2])/float(parts[3]))
	return parts[0], average
	


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda line: splittext(line)).distinct().groupByKey().cache()
    links2 = lines.map(lambda line: average(line)).distinct().groupByKey().cache()
    ranks = links2.map(lambda line: (line[0], max(1.0000000, sum(line[1]))))
    perc=float(sys.argv[3])
    a=1
    count=0
    if(int(sys.argv[2])==0):
        while(a > 0):
            contribs = links.join(ranks).flatMap(lambda bowler_rank: computeContribs(bowler_rank[1][0], bowler_rank[1][1]))
            n_rank = contribs.reduceByKey(add).mapValues(lambda rank: float(rank)*float(perc/100) + float(1-(perc/100)))
            count += 1
            ranks_sort=n_rank.sortBy(lambda x:x[0],ascending=True)
            new_rank=ranks_sort.sortBy(lambda x:x[1],ascending=False).collect()
            p_rank=ranks.sortBy(lambda x:x[0],ascending=True)
            prev_rank=p_rank.sortBy(lambda x:x[1],ascending=False).collect()
            ranks = n_rank
            if(abs(new_rank[0][1] - prev_rank[0][1]) < 0.00001):
                a = 0
    else:
        for iteration in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(lambda bowler_rank: computeContribs(bowler_rank[1][0], bowler_rank[1][1]))
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: float(rank)*float(perc/100) + float(1-(perc/100)))
    ranks_sort=ranks.sortBy(lambda x:x[0],ascending=True)
    ranks_finalsort=ranks_sort.sortBy(lambda x:x[1],ascending=False)
    for (link, rank) in ranks_finalsort.collect():
        print("%s,%s" % (link, format(rank,'.12f')))
    spark.stop()
