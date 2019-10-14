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
    return parts[1], round(float(parts[2])/float(parts[3]),12)


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

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey()
    links_one = lines.map(lambda urls: parseNeighbors1(urls)).distinct().groupByKey()
    b_persen = int(sys.argv[3])/100
    if(int(sys.argv[3]) == 0):
        b_persen = .80
    i = 0
    ranks = links_one.map(lambda url_neighbors: (url_neighbors[0], max(1,sum(url_neighbors[1]))))
    new_ranks_b = links_one.map(lambda url_neighbors: (url_neighbors[0], max(1,sum(url_neighbors[1]))))
    i=i+1
    contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    new_ranks_b = contribs.reduceByKey(add).mapValues(lambda rank: round(rank *b_persen + (1-b_persen),12))
            
    new =ranks.join(new_ranks_b)
            
    b_result = new.map(lambda x: abs(x[1][0] - x[1][1])).collect()
            

    sranks = new_ranks_b
    if(int(sys.argv[2]) == 0):
        while(not(max(b_result)<0.0001)):
            
            i=i+1
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            new_ranks_b = contribs.reduceByKey(add).mapValues(lambda rank: round(rank *b_persen + (1-b_persen),12))
            
            new =ranks.join(new_ranks_b)
            
            b_result = new.map(lambda x: abs(x[1][0] - x[1][1])).collect()
            

            ranks = new_ranks_b
            
    else:
        for iteration in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: round(rank *b_persen + (1-b_persen),12))
        i = int(sys.argv[2])  
    ranks = ranks.sortBy(lambda a : a[0])  
    new_ranks_b = ranks.sortBy(lambda a : -a[1])
    for (link, rank) in new_ranks_b.collect():
        print("%s,%s" % (link, rank))
    
    
    spark.stop()
