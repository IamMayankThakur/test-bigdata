from __future__ import print_function

import re
import sys
from operator import add
from pyspark.sql import SparkSession

def fun(urls):
	s=0    
	for i in urls:
		s=s+i[1]
	return max(1,s)
	
def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url[0], rank / num_urls)


def parseNeighbors(urls):
    l = re.split(r',', urls)
    return l[0], (l[1], float(l[2])/float(l[3]))

def check(ranks1, ranks2):
    l = ranks1.count()
    l1 = []
    l2 = []
    v = True
    for (link, rank) in ranks1.collect():
        l1.append(rank)
    for (link, rank) in ranks2.collect():
        l2.append(rank)
    for i in range(l):
        if(abs(l1[i]-l2[i]) > 0.0001):
            v = False
            #print("stop", round(abs(l1[i]-l2[i]), 4), round(abs(l1[i]-l2[i]), 4) > 0.0001, i)
            break
    return v
	
if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: <Path to python file> <Path to data file on HDFS> <Iterations> <Weights to default Rank>", file=sys.stderr)
		sys.exit(-1)

    # Initialize the spark context.
	spark = SparkSession\
		.builder\
		.appName("PythonBowlerBatsmenRank")\
		.getOrCreate()
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
	ranks = links.map(lambda url_neighbors: (url_neighbors[0], fun(url_neighbors[1])))
	if(int(sys.argv[2])==0):
		ranks0 = links.map(lambda url_neighbors: (url_neighbors[0], 0.0))
		i = 0
		while(check(ranks0.sortBy(lambda a:(a[0])), ranks.sortBy(lambda a:(a[0]))) == False):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks0 = ranks
			if(int(sys.argv[3])==0):
				weight=0.8
			else:
				weight=float(sys.argv[3])/100.0
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + 1-weight)
			i = i+1
		
	else:
		for i in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			if(int(sys.argv[3])==0):
				weight=0.8
			else:
				weight=float(sys.argv[3])/100.0
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + 1 - weight)
	ranks = ranks.sortBy(lambda a:(-a[1],a[0]))
	for (link, rank) in ranks.collect():
		rank = float(rank)
		print("%s, %.12f" % (link, rank))
	spark.stop()
