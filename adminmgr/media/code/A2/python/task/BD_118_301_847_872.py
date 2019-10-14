from __future__ import print_function

import re
import sys
from operator import add
import itertools


from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, float(rank / num_urls))


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = urls.split(',')
    return parts[0], parts[1]

def getRanks(urls):
    parts = urls.split(',')
    res=float(float(parts[2])/float(parts[3]))
    return parts[1], res

def set_r(urls):
    result=int(list(urls[1])[0])
    return urls[0], max(result,1)

if __name__ == "__main__":
    

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()



    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    links = lines.map(lambda urls: parseNeighbors(urls)).groupByKey() #.cache()
    temp_rank= lines.map(lambda urls: getRanks(urls)).groupByKey() #.cache()
    reduced= temp_rank.reduceByKey(add).cache()


    ranks=reduced.map(lambda p : set_r(p))
    tempo_ranks=ranks
    converge=int(sys.argv[2])
    weights=float(sys.argv[3])
    count=0
    if(converge==0):
        not_conv=1
        #convergence code
        while(not_conv==1):
            temp_r=ranks
            count=count+1
            
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            if(weights!=0): #not defult
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (weights/100) + (1-(weights/100)))
            
            else:
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
            
            r=temp_r.join(ranks)
            
            for i in r.collect():
                k=abs(float(i[1][0])-float(i[1][1]))
               
                if(float(k)<=0.00001):
                    
                    not_conv=0
                else:
                    not_conv=1
                    break

                
                    
            
            
            
    
    else:
        for i in range(converge):
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            if(weights!=0): #not defult
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (weights/100) + (1-(weights/100)))
            
            else:
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
    ranks=ranks.sortBy(lambda x:x[0],True)
    ranks=ranks.sortBy(lambda x:x[1],False)
    for (link, rank) in ranks.collect():
        print("%s,%.12f" % (link,float(rank)))
    spark.stop()
 


