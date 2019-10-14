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
    
def parseNeighbors2(urls):
    parts = urls.split(',')
    return (parts[1],int(parts[2])/int(parts[3]))

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

    ranks = lines.map(lambda urls: parseNeighbors2(urls)).reduceByKey(add).mapValues(lambda x: max(x,1)).cache()
    
    test = ranks.collect()
    print(test)
    print(int(sys.argv[3]))
    wt = int(sys.argv[3])* 0.01
    weight = int(sys.argv[3])
    print(weight)
    
    if int(sys.argv[3]) == 0:
	   
	   
	    for iteration in range(int(sys.argv[2])):
         	contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

	        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20) 
        
        newrank = ranks.sortBy(lambda x:(-x[1],x[0]))	
        for (link, rank) in newrank.collect():
            print("%s,%s" % (link, rank))
        
			
		
		
		
		
    else:
		    
			for iteration in range(int(sys.argv[2])):
        			 contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

         			 ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20) 
        
     		newrank = ranks.sortBy(lambda x:(-x[1],x[0]))	
		    for (link, rank) in newrank.collect():
                 print("%s,%s" % (link, rank))
    spark.stop()
