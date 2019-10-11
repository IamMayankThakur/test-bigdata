from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)
def parseNeighbors(urls):
    parts = re.split(r',', urls)
    return parts[0], parts[1]


def parseNeighbors1(urls):
    parts = re.split(r',', urls)
    return parts[1], float(parts[2])/float(parts[3])


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations> <rank>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    links_one = lines.map(lambda urls: parseNeighbors1(urls)).distinct().groupByKey().cache()
    per = int(sys.argv[3])/100
    i = 0
    ranks = links_one.map(lambda url_neighbors: (url_neighbors[0], max(1,sum(url_neighbors[1]))))
    ranks_New = links_one.map(lambda url_neighbors: (url_neighbors[0], max(1,sum(url_neighbors[1]))))
    result = [1 for i in range(len(ranks.collect()))]
    if(int(sys.argv[2]) == 0):
        while(max(result)>0.0001):
            
            i=i+1
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            
            if(int(sys.argv[3]) == 0):
                ranks_New = contribs.reduceByKey(add).mapValues(lambda rank: round(rank * 0.80 + 0.20,12))
            else:
                ranks_New = contribs.reduceByKey(add).mapValues(lambda rank: round(rank *per + (1-per),12))
            r = [rank for (link, rank) in ranks_New.collect()]
            new =ranks.join(ranks_New)
            result = [abs(p[0]-p[1]) for (b,p) in new.collect() ]
            ranks = ranks_New
            
    else:
        for iteration in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            if(per== 0):
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: round(rank * 0.80 + 0.20,12))
            else:
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: round(rank *per + (1-per),12))
        i = int(sys.argv[2])    
    ranks_New = ranks.sortBy(lambda a : -a[1])
    for (link, rank) in ranks_New.collect():
        print("%s has rank: %s." % (link, rank))
    print("iteration = ",i)
    
    spark.stop()
