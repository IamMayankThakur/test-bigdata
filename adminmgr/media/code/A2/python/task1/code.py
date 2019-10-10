
from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseWeights(urls):
    parts = urls.split(",")
    n = int(parts[2]) / int(parts[3])
    return parts[1], n

def parseUrls(urls):
    parts = urls.split(",")
    return parts[0], 0

def parseUrlsA(urls):
    parts = urls.split(",")
    return parts[1], 0

def parseNeighbors(urls):
    parts = urls.split(",")
    return parts[0], parts[1]

def addO(r):
    if r[1][1] is not None:
        return  r[0], r[1][1]
    else:
        return  r[0], r[1][0]

def addN(r):
    if r[1][1] is not None:
        return  r[0], r[1][1]
    else:
        return  r[0], 1

def diff(r):
    if r[1][0] is not None:
        if r[1][1] is not None:
            d = abs(r[1][0] - r[1][1])
            return r[0], d

def checkRank(r):
    if r[1] >= 1:
        return r[0],r[1]
    else:
        return r[0],1

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations> <weights>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    urls = lines.map(lambda urls: parseUrls(urls)).distinct()
    urls = urls.union(lines.map(lambda urls: parseUrlsA(urls)).distinct()).distinct().cache()
    
    ranks = lines.map(lambda urls: parseWeights(urls)).reduceByKey(add).map(lambda r: checkRank(r))
    ranks = urls.fullOuterJoin(ranks).map(lambda r: addN(r))
    
    wt = int(sys.argv[3])
    if wt == 0:
        wt = 0.8
    else:
        wt = float(wt/100)
    co = int(sys.argv[2])
    flag = 0
    iterations = 0
    if co == 0:
        while flag == 0 and iterations < 2000:
            iterations = iterations + 1
            prev_ranks = ranks
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*wt + (1 - wt))
            r1 = prev_ranks
            r2 = ranks
            flag = 1
            comp = r1.leftOuterJoin(r2).map(lambda r: diff(r))
            for x in comp.collect():
                if x is not None:
                    if x[1] >= 0.0001:
                        flag = 0
            if flag == 1:
                print("Converged")
    else:
        while iterations < co:
            iterations = iterations + 1
            prev_ranks = ranks
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*wt + (1 - wt))

    updatedRanks = urls.fullOuterJoin(ranks).map(lambda r: addO(r))
    sortedRanks = (updatedRanks.sortBy(lambda a: a[0])).sortBy(lambda a: -a[1])
    print("Iterations",iterations)
    print("Weights",wt)
    for (link, rank) in sortedRanks.collect():
        print("%s,%.12f" % (link, rank))

    spark.stop()
