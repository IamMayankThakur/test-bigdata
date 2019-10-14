from __future__ import print_function
from decimal import *
import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, float(rank / num_urls))

def parseNeighbors(urls):
    parts = re.split(r',', urls)
    return parts[0],parts[1]
def parseNeighbors1(urls):
    parts = re.split(r',', urls)
    return parts[0],float(int(parts[2])/int(parts[3]))
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
    if p==0:
        p=0.08
    if iterations!=0:
        for iteration in range(iterations):
            contribs = links.join(def_ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            def_ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank *p  + 1-p))
    else:
        ranks_test = [x for y,x in def_ranks.collect()]
        j=0
        count=len(ranks_test)
        l=count
        
        while l!=0: #and j<1:
            contribs = links.join(def_ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            #check1 = def_ranks.map(lambda x:x[1])
            c1 = def_ranks.map(lambda x:x) 
            def_ranks = contribs.reduceByKey(add).mapValues(lambda rank:(rank * 0.80 + 0.20))   
            #check2 = def_ranks.map(lambda x:x[1])
            c2 = def_ranks.map(lambda x:x)
            #diff = zip(check1,check2).map(lambda x:x[0]-x[1]).filter(lambda x: x<0.0001)
            diff=c1.join(c2).map(lambda x:abs(x[1][1]-x[1][0])).filter(lambda x:x>0.0001) 
            l = len(diff.collect())
            j+=1
            '''for i in check1.collect():
               print("check1",i)
            print("____________________________________")
            for i in check2.collect():
               print("check2",i)
            print("____________________________________")'''
            '''for i in diff.collect():
               print("diff",i)'''
		
        #print("MAX OF DIFF",max(ranks_test))
        #print("MIN OF DIFF",min(ranks_test))
        print("____________________________________")
    sort = def_ranks.sortBy(lambda a: (-a[1],a[0]))
    for (link, rank) in sort.collect():
           print("%s has rank: %.12f" % (link, rank))

    spark.stop()
