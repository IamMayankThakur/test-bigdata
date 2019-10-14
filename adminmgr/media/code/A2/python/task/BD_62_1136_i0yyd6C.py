from __future__ import print_function
import re
import sys
from operator import add

from pyspark.sql import SparkSession
temp=int(sys.argv[3])
def bowlingaverage(bats):
  mysum=0     
  for b in bats:
        w=float(b[1])
        d=float(b[2])
        avg=w/d
        mysum+=avg   
  return mysum

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url[0], rank / num_urls)

def parseNeighbors(urls):
    parts = re.split(r'\,', urls)
    return parts[0],(parts[1],parts[2],parts[3])


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations> ", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
  
    
    links = lines.map(lambda urls: parseNeighbors(urls)).groupByKey().cache()
   
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], max(1.0,bowlingaverage(list(url_neighbors[1])))))
   
    if(int(sys.argv[2])==0):
        while 1:
	    prev=ranks
	    contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            if(sys.argv[3]=='0'):
	         ranks = contribs.reduceByKey(add).mapValues(lambda rank : rank * 0.80 + 0.20)
	    else:
		 ranks = contribs.reduceByKey(add).mapValues(lambda rank : (rank *temp/100) + (100-temp)/100)
	    
	    t=prev.join(ranks).map(lambda x:abs(x[1][0]-x[1][1])).filter(lambda x:x<0.0001).count()
            if t==ranks.count():
		break    
    else:
        for iteration in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
	    if(sys.argv[3]=='0'):
	         ranks = contribs.reduceByKey(add).mapValues(lambda rank : rank * 0.80 + 0.20)
	    else:
		 ranks = contribs.reduceByKey(add).mapValues(lambda rank : (rank *temp/100) + (100-temp)/100)
    ranks = ranks.sortBy(lambda x : (-x[1],x[0]))
    for (bo, rank) in ranks.collect():
        print(bo,"{:.12f}".format(rank),sep = ",")
   

    spark.stop()
