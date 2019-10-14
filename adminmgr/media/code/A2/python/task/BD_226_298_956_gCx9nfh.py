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
    parts = urls.split(',')
    return parts[0], parts[1]

def getAvg(urls):
    parts = urls.split(',')
    return parts[0], float(parts[2])/float(parts[3])

def getSum(urls):
    s=0
    for i in urls[1]:
        s=s+i

    return urls[0],s

def getRank(urls, l):
    for i in l:
        if(i[0]==urls):
            return i[1]



if __name__ == "__main__":
    

    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
    if(int(sys.argv[3])==0):
        x=0.8
    else:
        x=float(sys.argv[3])/100
    y = 1-x
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    linksn= lines.map(lambda urls:getAvg(urls)).distinct().groupByKey().cache()
    linksn =linksn.map(lambda urls:getSum(urls))

    l=linksn.collect()
    
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], max(getRank(url_neighbors[0],l),1)))
    if(int(sys.argv[2])==0):
        data = dict()
        for (link,rank) in ranks.collect():
            data[link]=rank
            
        temp = 0
        while(not temp):
            
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))


            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * x + y)
            
            new_rank = dict()
            i = 0
            temp = 1
            for (link, rank) in ranks.collect():
                if(link in data and data[link] - rank > 0.0001):
                    temp = 0
                new_rank[link]=rank
                i = i + 1
            if(temp == 1):
                break
            data = new_rank
    else:
        for i in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * x + y)        

    ranks = ranks.sortBy(lambda a: (-a[1],a[0]))
    for (link, rank) in ranks.collect():
        print("%s,%.12f" %(link, rank))

    spark.stop()
