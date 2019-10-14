from __future__ import print_function

import math
def truncate(number, digits) -> float:
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

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
	bow_avg = float(parts[2])/float(parts[3])
	return parts[1], bow_avg

def parseNeighbors2(urls):
	parts = re.split(r',', urls)
	return parts[0], parts[1]

def func(x):
	if(x <= 0.0001):
		return False
	else:
		return True


if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: pagerank <file> <iterations> <weight>", file=sys.stderr)
		sys.exit(-1)

	# Initialize the spark context.
	spark = SparkSession\
		.builder\
		.appName("PythonPageRank")\
		.getOrCreate()

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	#print(lines.collect())

	links = lines.map(lambda urls: parseNeighbors2(urls)).distinct().groupByKey()

	ranks_prev1 = lines.map(lambda urls: parseNeighbors(urls)).distinct().reduceByKey(lambda a,b: a+b)

	ranks_prev = ranks_prev1.mapValues(lambda x: x if x>1 else 1)

	#ranks_prev = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
	i=1
	c=0
	if(int(sys.argv[2]) == 0):

		if(int(sys.argv[3]) == 0):
			weight = 0.80
			
		else:
			weight = sys.argv[3]/100

		while(i):
				c=c+1

				contribs = links.join(ranks_prev).flatMap(
					lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

				ranks_now = contribs.reduceByKey(add).join(ranks_prev).mapValues(lambda rank: (rank[0] * weight + (1-weight)))

				diff = ranks_now.join(ranks_prev).mapValues(lambda x: x[1]-x[0])
		 
				diff_values = diff.values()
		   
				diff_nconverge = diff_values.filter(func)
			   
				empty = diff_nconverge.count()
			 
				if(empty == 0):
					i=0
				else:
					ranks_prev = ranks_now
			
	else:

		if(int(sys.argv[3]) == 0):
			weight = 0.80
			
		else:
			weight = int(sys.argv[3])/100

		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks_prev).flatMap(
				lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

			ranks_now = contribs.reduceByKey(add).join(ranks_prev).mapValues(lambda rank: (rank[0] * weight + (1-weight)))
			ranks_prev = ranks_now


	for (link, rank) in ranks_now.sortByKey().sortBy(lambda a:-a[1]).collect():
		print("%s, %.12f" % (link, float(rank)))
	print(c)

	spark.stop()
