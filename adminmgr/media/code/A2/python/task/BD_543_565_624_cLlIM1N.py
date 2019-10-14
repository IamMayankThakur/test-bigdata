from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def retavg(bowler,ba):
        for i in (ba):
            if i[0]==bowler:
                return max(1,float(i[1]))

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls,x):
    """Parses a urls pair string into urls pair."""
    parts = urls.split(",")
    avg=float(parts[2])/float(parts[3])
    if x==1:
        return parts[0], parts[1]
    return parts[1],avg

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    n=int(sys.argv[2])
    if (int(sys.argv[3])==0):
        w1=0.80
        w2=0.20
    else:
        w1=int(sys.argv[3])/100
        w2=1-w1
    #print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
    #     "Please refer to PageRank implementation provided by graphx",
    #      file=sys.stderr)

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
    #print("lines", lines.collect())
    # Loads all URLs from input file and initialize their neighbors.
    temp=lines.map(lambda urls: parseNeighbors(urls,0)).distinct().groupByKey().cache()
    #print("\n\n\n temp\n",temp.collect())
    
    
    ba=temp.map(lambda url: (url[0],sum(url[1])))

    links = lines.map(lambda urls: parseNeighbors(urls,1)).distinct().groupByKey().cache()
    #print("\n\n\nlinks\n",links.collect())
    #print("\n\n\n\n",links.collect()[0][1].collect() )
    
    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    
    
				
    x= ba.collect()
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], retavg(url_neighbors[0],x)))

    #ranks=links.map(findsum)
    #print("\n\n\nranks\n",ranks.collect())
    # Calculates and updates URL ranks continuously using PageRank algorithm. 
    # j=links.join(ranks).collect()
    #print("\n\n\nJoin \n",j)
    if(n==0):
        count=1
        while(count!=0):

            contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            #print("\n\n\ncontrib\n",contribs.collect())
            # Re-calculates URL ranks based on neighbor contributions.
            count=0
            prev=ranks
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * w1 +w2)
            
            d={}
            for row in prev.collect():
                d[row[0]]=row[1]

             
            for i,j in zip(ranks.collect(),prev.collect()):
                if(abs(i[1]- j[1])>=0.0001):
                    count+=1
                    break
            if count==0:
                break
    else:
            #print("\n\n\nranksnew\n",ranks.collect())
            # Collects all URL ranks and dump them to console.


        for iteration in range(int(n)):
            # Calculates URL contributions to the rank of other URLs.
            #contribs=links.join(ranks)

            #print("\n\n\njoin\n",contribs)
    
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            #print("\n\n\ncontrib\n",contribs.collect())
            # Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * w1 +w2)
            #print("\n\n\nranksnew\n",ranks.collect())
            # Collects all URL ranks and dump them to console.

   # print(ranks.takeOrdered(ranks.count(),key=lambda x:(-x[1],x[0])))
    for (link, rank) in ranks.takeOrdered(ranks.count(),key=lambda x:(-x[1],x[0])):
        print("%s,%.12f" % (link, rank))
    
    spark.stop()
    
