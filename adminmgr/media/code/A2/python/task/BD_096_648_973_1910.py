from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(lines):
    parts = re.split(r',', lines)
    return parts[0], float(parts[2])/float(parts[3])
    
def parseNeigh(lines):
    parts = re.split(r',', lines)
    return parts[0], parts[1]

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations> <weight>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    batsmen = lines.map(lambda lin: parseNeigh(lin)).distinct().groupByKey().cache()

    ranks = lines.map(lambda lin: parseNeighbors(lin)).distinct().reduceByKey(add).cache()
    ranks = ranks.map(lambda r:(r[0],1) if r[1]<1 else r)
    if(float(sys.argv[3])==0):
    	w=0.8
    else:
    	w=float(sys.argv[3])/100
    
    #for (bow,rank) in bowlers.collect():
    #    print(bow)
    #    for i in rank:
    #   	print(i)
        
    ranksp=ranks.mapValues(lambda r:0)
    
    #for (bow,rank) in ranksp.collect():
    #    print("%s has rank: %s." % (bow, rank))

    for iteration in range(int(sys.argv[2])):
        contribs = batsmen.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * w + (1-w))
        
    if(int(sys.argv[2])==0):
        while(True):
            contribs = batsmen.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * w + (1-w))
            a=ranks.join(ranksp)
            chk=0
            for (link,rank) in a.collect():
                #print(link,rank[0],rank[1])
                if(abs(rank[0]-rank[1])>0.0001):
                    chk=1
                    break
            if(chk==1):
                ranksp=ranks
                continue
            else:
                break
        	
			
    ranks=ranks.takeOrdered(len(ranks.collect()),key=lambda x: (-x[1],x[0]))
    for (link, rank) in ranks:
        #print(1)
        print("%s,%s" % (link, rank))

    spark.stop()
