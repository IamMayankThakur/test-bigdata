from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, float (rank) /float(num_urls))

def compute(key,val):
    if(val>1):
        return key,val
    return key,1

def parseNeighbors(urls):
    parts = re.split(r',', urls)
    return parts[0],float(parts[2])/float(parts[3])

def parseNeigbors1(urls):
    parts = re.split(r',',urls)
    return parts[0],parts[1]

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)
    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
    some_value = float((float(sys.argv[3]))/100)
    if (some_value == 0):
		some_value = 0.8
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    links2 = lines.map(lambda urls: parseNeighbors(urls)).groupByKey().mapValues(sum).cache()
    ranks=links2.map(lambda x:compute(x[0],x[1]))
    prevranks=links2.map(lambda x:compute(x[0],x[1]))
    links1=lines.map(lambda urls: parseNeigbors1(urls)).groupByKey().cache()
    
    count_value = 0 
    count = 0
    t = True

    if(int(sys.argv[2]) != 0):
    	t = False
    	for iteration in range(int(sys.argv[2])):
			contribs = links1.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (some_value) + (1-some_value))
			
			
    contribs = links1.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    prevranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (float(some_value)) + (float(1-some_value)))
	    
    while(t):
        count = 0
        count_value = 0

        contribs = links1.join(prevranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (some_value) + (1-some_value))
        
        temp = ranks.join(prevranks)
        
        for i in temp.collect():
        	if(abs(i[1][0]-i[1][1])<0.0001):
        		count_value+=1
        for i in ranks.collect():
			count+=1
        if(count == count_value):
			t = False
        prevranks = ranks
    
    result=sorted(ranks.collect(),key=lambda x:(-x[1],x[0]))
    for (link, rank) in result:
        print(link,"{0:.12f}".format(rank),sep=",")
        #print("%s,%s" % (link, rank))
    spark.stop()
