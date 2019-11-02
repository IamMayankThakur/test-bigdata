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
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: pagerank <file>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    length = lines.count()

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    prev_ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    res = [1 for i in range(length) ]
    i = 0
    while(1):
        i+=1
        contribs = links.join(prev_ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
        r = [rank for(link, rank) in ranks.collect()]
        p = [rank for(link, rank) in prev_ranks.collect()]
        res = [1 for i in range(len(r)) ]
        for i in range(len(r)):
        	res[i] =r[i]-p[i]
        if(max(res)<0.0001):
        	break		
        else:
        	prev_ranks = ranks

    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))
    print(i)

    spark.stop()
