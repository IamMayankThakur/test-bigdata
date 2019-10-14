
from __future__ import print_function
import time
import re
import sys
from operator import add

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

def parseNeighbors_rank(urls):
    
    """Parses a urls pair string into urls pair."""
    parts = urls.split(',')
    res=float(float(parts[2])/float(parts[3]))
    return parts[1], res

if __name__ == "__main__":
    

    
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

   
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    #print(lines.collect())
    links = lines.map(lambda urls: parseNeighbors(urls)).groupByKey().cache()
    temp_rank= lines.map(lambda urls: parseNeighbors_rank(urls)).cache()
    consol= temp_rank.reduceByKey(add).cache()

    ranks = consol.map(lambda url_neighbors: (url_neighbors[0], max(url_neighbors[1],1)))
    
    tempo_ranks=ranks
    count=0


    if(int(sys.argv[2])!=0):
        for iteration in range(int(sys.argv[2])):

            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            if(int(sys.argv[3])==0):
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
            else:
                x=int(sys.argv[3])
                x=x/100
                y=1-x
                ranks= contribs.reduceByKey(add).mapValues(lambda rank: rank * x + y)

    else:
        while(True):
            count=count+1
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            if(int(sys.argv[3])==0):
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
            else:
                x=int(sys.argv[3])
                x=x/100
                y=1-x

                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * x + y)
            tmp_ranks=tempo_ranks.collect()
            tempo_ranks=tempo_ranks.sortBy(lambda x:x[0],True)
            #tempo_ranks=tempo_ranks.sortBy(lambda x:x[1],False)
            #ranked_t=ranks.collect()
            ranks=ranks.sortBy(lambda x:x[0],True)
            #ranks=ranks.sortBy(lambda x:x[1],False)

            flag=1
            run=1
	     
            for i,j in zip(tempo_ranks.collect(),ranks.collect()):
                #run=run+1
               	#if(run==9):
                    #break;
                #a=float(j[1])-float(i[1])
                #print(str(j[1])+"-"+str(i[1])+str(count),file=open("assg7.txt","a"))
                if(abs(float(j[1])-float(i[1]))>=0.0001):
                    flag=0
                  
            if(flag==1):
                break;
            else:
                tempo_ranks=ranks


    ranks=ranks.sortBy(lambda x:x[0],True)
    ranks=ranks.sortBy(lambda x:x[1],False)
    for (link, rank) in ranks.collect():
        print("%s,%.12f" % (link,float(rank)))
	
    
    spark.stop()

