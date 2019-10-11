from __future__ import print_function

import re
import sys
from operator import add
import time

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url[0], rank / num_urls)


def getRank(url_neighbours):
    l = list(url_neighbours[1])
    init_rank = 0
    for i in l:
        init_rank += i[1]
    if init_rank < 1:
        init_rank = 1 
    return (url_neighbours[0], init_rank)


def parseNeighbors(urls):
    parts = urls.split(",")
    temp = int(parts[2])/int(parts[3])
    return parts[0], (parts[1], temp)


def converged(new, old, thresh):
    c = old.join(new).map(lambda url_old_new: abs(url_old_new[1][0] - url_old_new[1][1])).filter(lambda x:x<thresh).count()
    if c == new.count():
        return True
    return False

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    
    ranks = links.map(getRank)

    iterations = int(sys.argv[2])
    w_cal_rank = int(sys.argv[3]) * 0.01
    w_def_rank = 0
    if w_cal_rank == 0:
        w_cal_rank = 0.8
    w_def_rank = 1-w_cal_rank

    if iterations > 0:
        for iteration in range(iterations):
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * w_cal_rank + w_def_rank)
    
    elif iterations == 0:
        old_ranks = None
        convergeCount = 0
        t1 = time.time()
        while 1:
            convergeCount += 1
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            old_ranks = ranks
        
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * w_cal_rank + w_def_rank)
            if converged(old_ranks, ranks, 0.0001):
                break
            t2 = time.time()
        iterations = convergeCount
        
    ranks = ranks.sortBy(lambda x:x[1], ascending=False)
    for (link, rank) in ranks.collect():
        print("%s,%s" % (link, rank))
    #print("Number of iterations:", iterations)   
    #print("Time Taken:", t2-t1, "s")

    spark.stop()
