from __future__ import print_function
from decimal import *
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
    return parts[0],parts[1]
def parseNeighbors1(urls):
    parts = re.split(r',', urls)
    return parts[1],round(float(parts[2])/float(parts[3]),12)
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
    
    #to calculate default rank
    lines1 = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    links1 = lines1.map(lambda urls: parseNeighbors1(urls)).distinct().groupByKey().cache()
    #def_ranks = links1.map(lambda x:(x[0],max(round(sum(x[1]),12),1.0)))
    #default rank calculated
    def_ranks = links1.map(lambda x:(x[0],max(sum(x[1]),1.0)))
    
    iterations = int(sys.argv[2])
    p = float(sys.argv[3])/100
    if iterations!=0:
        for iteration in range(iterations):
            contribs = links.join(def_ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            def_ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank *p  + 1-p))
    else:
        j=0
      
        l=2
        
        while l!=0: 
            contribs = links.join(def_ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            
            c1 = def_ranks.map(lambda x:x).sortByKey() 
            def_ranks = contribs.reduceByKey(add).mapValues(lambda rank:(rank * 0.80 + 0.20))   
            
            c2 = def_ranks.map(lambda x:x).sortByKey()
       
            diff=c1.join(c2).map(lambda x:abs(x[1][1]-x[1][0])).filter(lambda x:x>0.0001) 
            l = len(diff.collect())
    sort = def_ranks.sortBy(lambda a: (-a[1],a[0]))
    for (link, rank) in sort.collect():
           print("%s,%s" % (link, rank))

    spark.stop()
