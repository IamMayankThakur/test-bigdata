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

def computeWeights(urls):
    parts = urls.split(",")
    return parts[0], int(parts[2]) / int(parts[3]

def setRank(x):
    return (x[0], max(x[1],1))



if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations> <weights>", file=sys.stderr)
        sys.exit(-1)
    
    if(int(sys.argv[3])==0):
        weight = 0.8
    
    if(int(sys.argv[3])>0):
        weight = float(sys.argv[3])/100


    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    intermediate_ranks = lines.map(lambda urls: computeWeights(urls)).distinct().reduceByKey(add)
    ranks=intermediate_ranks.map(lambda x:(setRank(x))).sortBy(lambda x:(x[1],x[0]),False)

    #Non Convergence case:

    if(int(sys.argv[2]!=0)):
        for iteration in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*weight + (1-weight)).sortBy(lambda x:(x[1],x[0]),False)


    #Convergence case:

    if (int(sys.argv[2]) == 0):
        flag=0
        iteration =0
        for iteration in range(2000):

            contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            prev_rank = ranks.sortBy(lambda x:(x[1],x[0]),False).collect()
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*weight + (1-weight)).sortBy(lambda x:(x[1],x[0]),False)

            flag = 1
            conv = prev_rank[1][1] - ranks.collect()[1][1]

            if(abs(conv)>=0.0001):
                flag=0

            if(flag==1):
                print("Converged")
                break


    #Printing the ranks:

    for (link, rank) in ranks.collect():
        print("%s,%.14s" % (link, rank))

    spark.stop()
