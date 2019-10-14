from __future__ import print_function
import sys
import re
from operator import add
from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(p):
    parts = re.split(r",",p)
    return parts[0], parts[1]

def parseNeighbors1(p):
    parts=re.split(r",",p)
    return parts[1],(float(parts[2])/float(parts[3]))


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("BatsmanBowlerRank")\
        .getOrCreate()

#Preprocressing

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0]) #Read all lines
    links = lines.map(lambda p:parseNeighbors(p)).distinct().groupByKey() #corresponding values for each key
    ranks = lines.map(lambda p: parseNeighbors1(p)).groupByKey().mapValues(sum) #finding initial ranks
    for (bb, rank) in ranks.collect():
        rank=max(rank,1.0)
    if(int(sys.argv[3])==0):
        w1=0.80
        w2=0.20
    else:
        w1=float(sys.argv[3])/100
        w2=1-w1

#Iterations/Convergence

    if(int(sys.argv[2])!=0):
        for iteration in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * w1 + w2)
    else:
        noit=0    
        converged=False
        while not converged:
            storedoldranks=ranks #storing ranks
            oldranks=ranks.collect() 
            contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * w1 + w2)
            ranks1=ranks.collect()
            iterations=0
            while(iterations < storedoldranks.count() and iterations!=-1):
                diff=abs(ranks1[iterations][1]-oldranks[iterations][1]) #Difference for convergence
                if(diff<0.0001):
                    iterations=iterations+1
                else:
                    iterations=-1
            if(iterations!=-1):
                converged=True
                ranks=storedoldranks
            noit=noit+1          


#Output display
    rs=ranks.sortBy(lambda info: (-info[1],info[0])) #Sorting based on descending order
    for (bb, rank) in rs.collect():
        print("%s,%.12f" % (bb,round(rank,12)))
    spark.stop()
