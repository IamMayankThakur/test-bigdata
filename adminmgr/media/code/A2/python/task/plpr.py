from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r',', urls)
    return parts[0], parts[1]

def getValues(urls):
    parts = re.split(r',', urls)
    return parts[0], float(parts[2])/float(parts[3])

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations> <weights>", file=sys.stderr)
        sys.exit(-1)

    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx",
          file=sys.stderr)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)
                      ).distinct().groupByKey().cache()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = lines.map(lambda x: getValues(x)).reduceByKey(add)
    ranks = ranks.mapValues(lambda rank: rank if rank > 1.0 else 1.0)
    N = ranks.count()
    iterations = int(sys.argv[2])
    weight = float(sys.argv[3])
    if(iterations==0):
        while(1):
            oldRanks = ranks
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            #print(contribs.collect())  
            ranks = contribs.reduceByKey(add).mapValues(
                lambda rank: rank * weight + (1-weight))
            s = 0
            test = oldRanks.join(ranks).map(lambda r: abs(r[1][0]-r[1][1]))
            for i in test.collect():
                s += i
            if(s < N*0.0001):
                break
    else:
        for iteration in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks = contribs.reduceByKey(add).mapValues(
                lambda rank: rank * weight + (1-weight))

    #ranks1 = sorted(ranks, key = ranks.map(lambda x: (-x[1],x[0])))

    
    # Collects all URL ranks and dump them to console.
    for (link, rank) in ranks.sortBy(lambda x: (-x[1],x[0])).collect():
        print("%s,%s." % (link, rank))

    spark.stop()
