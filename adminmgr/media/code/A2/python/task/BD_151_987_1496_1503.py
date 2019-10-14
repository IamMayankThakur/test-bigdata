from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def computeAvgs(urls):
    parts = re.split(',',urls)
    return parts[1],float(parts[2])/float(parts[3])

def getplayers(players):
    parts = re.split(',',players)
    return parts[1]


def computeRanks(runs_balls):

    if runs_balls < 1:
        return 1.0

    return runs_balls


def parseNeighbors(urls):
    parts = re.split(',', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    weight = float(sys.argv[3])/100
    
    if float(sys.argv[3]) == 0:
        weight = 0.80

    

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    avgs = lines.map(lambda urls:computeAvgs(urls)).reduceByKey(lambda x,y:float(x) + float(y)).cache()

    ranks = avgs.mapValues(lambda runs_balls: computeRanks(runs_balls)).cache()


    if int(sys.argv[2]) != 0:
        for iteration in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + 1-weight)

    else:
        prev_ranks = lines.map(lambda players:getplayers(players)).distinct().map(lambda p: (p,0.0))
        number_of_batsmen = len(prev_ranks.collect())
        ranks_joined = ranks.join(prev_ranks).mapValues(lambda q: abs(float(q[0]) - float(q[1]))).filter(lambda p:float(p[1]) < 0.0001)
        filtered_ranks = len(ranks_joined.collect())

        while filtered_ranks < number_of_batsmen:
            contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            prev_ranks = ranks
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1-weight))
            ranks_joined = ranks.join(prev_ranks).mapValues(lambda q: abs(float(q[0]) - float(q[1]))).filter(lambda p:float(p[1]) < 0.0001)
            filtered_ranks = ranks_joined.count()

    
    final_ranks = ranks.sortBy(lambda a:(-a[1],a[0]))

        
    for (batsmen, rank) in final_ranks.collect():
        print("%s,%.12f" % (batsmen,rank))

    spark.stop()
