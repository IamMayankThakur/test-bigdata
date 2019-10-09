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

def parseWeights(urls):
    """Parses a urls pair string into urls pair."""
    parts = urls.split(",")
    #print(parts[0])
    n = int(parts[2]) / int(parts[3])
    return parts[1], n

def parseUrls(urls):
    parts = urls.split(",")
    return parts[0], 0

def parseUrlsA(urls):
    parts = urls.split(",")
    return parts[1], 0

def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
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
            #print(r)
            #print("diff",d)
            return r[0], d

def checkRank(r):
    if r[1] >= 1:
        return r[0],r[1]
    else:
        return r[0],1

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file>", file=sys.stderr)
        sys.exit(-1)

    
    input_iterations = sys.argv[2]
    input_weight = sys.argv[3]

    total_iterations = 2000
    weight = 0.8

    if input_iterations != 0:
        total_iterations = input_iterations
    
    if input_weight != 0:
        weight = input_weight / 100


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
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    urls = lines.map(lambda urls: parseUrls(urls)).distinct()
    urls = urls.union(lines.map(lambda urls: parseUrlsA(urls)).distinct()).distinct().cache()
    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = lines.map(lambda urls: parseWeights(urls)).reduceByKey(add).map(lambda r: checkRank(r))
    ranks = urls.fullOuterJoin(ranks).map(lambda r: addN(r))
    # Calculates and updates URL ranks continuously using PageRank algorithm.

    flag = 0
    iterations = 0


    while flag == 0 and iterations < total_iterations:
        iterations = iterations + 1
        #print("Iteration - ",iterations)
        prev_ranks = ranks
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*weight + 0.2)
        r1 = prev_ranks
        r2 = ranks
        #zipped = zip(r1.map(_._2), r2.map(_._2))
        #sample = zipped.map(lambda x: x._1-x._2)
        flag = 1
        #print(len(r1),len(r2))
        comp = r1.leftOuterJoin(r2).map(lambda r: diff(r))
        for x in comp.collect():
            #print(x)
            if x is not None:
                #print(x[1])
                if x[1] >= 0.0001:
                    flag = 0

    updatedRanks = urls.fullOuterJoin(ranks).map(lambda r: addO(r))
    sortedRanks = (updatedRanks.sortBy(lambda a: a[0])).sortBy(lambda a: -a[1])
    #print("Wo")
    #print(x.collect)
    #print(x.collect())
    # Collects all URL ranks and dump them to console.
    #print("Number of Iterations executed",iterations)
    
    for (link, rank) in sortedRanks.collect():
        print("%s,%.12f" % (link, rank))

    spark.stop()
