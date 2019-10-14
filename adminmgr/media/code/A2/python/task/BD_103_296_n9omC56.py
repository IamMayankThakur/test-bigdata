from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession



def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url,rank / num_urls)

def parseNeighbours(pairs):
    parts = pairs.split(',')
    return parts[0], parts[1]

def parsePairs(pairs):
    parts = pairs.split(',')
    return parts[1], float(parts[2])/float(parts[3])

def initRank(pairs):
    return pairs[0], max(sum(pairs[1]), 1.00)

if __name__ == "__main__":

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    n = int(sys.argv[2])
    weight = int(sys.argv[3])
    if (weight == 0):
        weight = 0.80
    else:
        weight = weight/100

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda pairs: parseNeighbours(pairs)).distinct().groupByKey().cache()

    ranks = lines.map(lambda pairs: parsePairs(pairs)).distinct().groupByKey().cache()
    ranks = ranks.map(lambda pairs: initRank(pairs))

    if(n!=0):
        for i in range(n):
            contribs = links.join(ranks).flatMap(
                lambda bowler_bowlers_rank: computeContribs(bowler_bowlers_rank[1][0], bowler_bowlers_rank[1][1]))
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1.00-weight))
    else:
        flag = 0
        while(flag==0):
            counts = 0
            prev_ranks = ranks
            contribs = links.join(ranks).flatMap(lambda bowler_bowlers_rank: computeContribs(bowler_bowlers_rank[1][0], bowler_bowlers_rank[1][1]))
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1.00-weight))
            new_ranks = prev_ranks.join(ranks)
            for rank in new_ranks.collect():
                if(round(rank[1][0], 4) == round(rank[1][1], 4)):
                    counts = counts + 1
            if(counts==ranks.count()):
                flag = 1 

    for (link, rank) in sorted(ranks.collect(), reverse=True, key = lambda x: x[1]): 
        print("%s,%.12f"%(link, rank))


    spark.stop()


