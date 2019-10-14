from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def parseNeighbors1(lines):
	parts = re.split(r',', lines)
	return parts[0], float(parts[2])/float(parts[3])

def computeContribs(urls, rank):
	num_urls = len(urls)
	for url in urls:
		yield (url, rank / num_urls)
    
def parseNeighbors2(lines):
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
	x=float(sys.argv[3])

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	ranks_init = lines.map(lambda url_neighbors: parseNeighbors1(url_neighbors)).distinct().reduceByKey(add)
	ranks = ranks_init.map(lambda tu:(tu[0],1) if tu[1]<1 else tu)
	links= lines.map(lambda url_neighbors: parseNeighbors2(url_neighbors)).distinct().groupByKey()

	if(int(sys.argv[2])!=0):
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			if(x>0):
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (x/100) + (1-(x/100)))
			else:
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
        
	elif(int(sys.argv[2])==0):
		while(True):
			previous_ranks=ranks
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			if(x>0):
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (x/100) + (1-(x/100)))
			else:
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
			rankss=ranks.join(previous_ranks)
			flag=1
			for i in rankss.collect():
				if(abs(i[1][0]-i[1][1])>0.0001):
					flag=0
					break
			if(flag==1):
				break
        				
	ranks=ranks.sortBy(lambda k:(-k[1],k[0]))
	for (link, rank) in ranks.collect():
		print("%s,%.12f" % (link, rank))

	spark.stop()
