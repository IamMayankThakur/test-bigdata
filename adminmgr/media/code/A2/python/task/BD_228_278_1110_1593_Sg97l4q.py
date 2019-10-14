from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession
def average(bowler,li):
    sum=0
    for i in li:
        if(i[1]==bowler):
            sum+=i[2]/i[3]
    return sum

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors1(urls):
    """Parses a urls pair string into urls pair."""
    parts = list(map(str,urls.split(",")))
    return parts[0],parts[1]

def parseNeighbors2(urls):
    parts = list(map(str,urls.split(",")))
    return parts[1],float(parts[2])/float(parts[3])

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)
    num_iter=int(sys.argv[2])
    x=float(sys.argv[3])

    #print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          #"Please refer to PageRank implementation provided by graphx",
          #file=sys.stderr)

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
    links1 = lines.map(lambda urls: parseNeighbors2(urls)).distinct().groupByKey()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links1.map(lambda url_neighbors: (url_neighbors[0],max(sum(url_neighbors[1]),1.0)))
    links=lines.map(lambda urls: parseNeighbors1(urls)).distinct().groupByKey()
   
    if(num_iter!=0): 
    # Calculates and updates URL ranks continuously using PageRank algorithm.
        for iteration in range(int(sys.argv[2])):
        # Calculates URL contributions to the rank of other URLs.
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        # Re-calculates URL ranks based on neighbor contributions.
            if(x==0.0):
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
            else:
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*(0.01*x) + (1-0.01*x))

    else:
        while(True):
	    previous_ranks=ranks
	    contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        # Re-calculates URL ranks based on neighbor contributions.
            if(x==0):
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
                flag=1
	        rankss=ranks.sortBy(lambda k:k[0]).collect()
		previous_rankss=previous_ranks.sortBy(lambda k:k[0]).collect()
	        for i in range(len(rankss)):
	              if(abs(rankss[i][1]-previous_rankss[i][1])>=0.0001):
		         flag=0
		         break
	        if(flag==1):
	           break
            else:
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*(0.01*x) + (1-0.01*x))
	        flag=1
	        rankss=ranks.sortBy(lambda k:k[0]).collect()
		previous_rankss=previous_ranks.sortBy(lambda k:k[0]).collect()
	        for i in range(len(rankss)):
	              if(abs(rankss[i][1]-previous_rankss[i][1])>=0.0001):
		         flag=0
		       	 break
	        if(flag==1):
	           break
 # Collects all URL ranks and dump them to console.
    ranks=ranks.sortBy(lambda k:(-k[1],k[0]))
    for (link, rank) in ranks.collect():
        print("%s,%.12f" % (link, rank))   
    spark.stop()
