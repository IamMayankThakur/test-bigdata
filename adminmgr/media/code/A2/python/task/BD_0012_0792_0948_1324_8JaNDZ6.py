from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession

def computeContribs(url, rank):
	num_urls = len(url)
	for i in url:
        	yield (i, rank / num_urls)

def parseNeighbors(url):
	prts = re.split(r',', url)
	avg = int(prts[2])/int(prts[3])
	return prts[1],avg

def parseNeighbors1(url):
	prts = re.split(r',', url)
	return prts[0], prts[1]

def converge(o_rank,n_rank):
	o=o_rank.collect()
	n=n_rank.collect()
	cnt=0
	has_converged = 0
	i = 0
	rank_sort1 = o_rank.sortBy(lambda a: -a[0])
	rank_sort2 = n_rank.sortBy(lambda a: -a[0])
	while i < len(n) and not has_converged:
		if (rank_sort1[i][1]-ranksort2[i][1] < 0.0001):
			cnt+=1
			has_converged &= 1
		else:
			has_converged &= 0
		i+=1
	
	return has_converged

if __name__ == "__main__":
	print("Start")
	if len(sys.argv) != 4:
        	print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        	sys.exit(-1)


# Initialize the spark context.
	spark = SparkSession\
		.builder\
		.appName("PythonPageRank")\
		.getOrCreate()
	linss = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	ranks = linss.map(lambda url: parseNeighbors(url)).distinct().groupByKey().mapValues(sum).cache()
	ranks_new=ranks.mapValues(lambda x:max(x,1.0))
	links = linss.map(lambda url: parseNeighbors1(url)).distinct().groupByKey().cache()

	weight=float(sys.argv[3])
	num_iters = 0
	if(int(sys.argv[2]) == 0):
		o_rank=None
		while True:
			num_iters+=1
			contributs = links.join(ranks_new).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			o_rank=ranks_new
			ranks = contributs.reduceByKey(add).mapValues(lambda rank: rank * weight + 1-weight)
			n_rank=ranks
			if converge(o_rank,n_rank) :
				break
			o_rank = n_rank

	elif(int(sys.argv[2]) > 0):
		for iteration in range(int(sys.argv[2])):
			contributs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contributs.reduceByKey(add).mapValues(lambda rank: rank * weight + 1-weight)

	rank_sort = ranks.sortBy(lambda a: -a[1])

	for (link, rank) in rank_sort.collect():
		print("%s,%s." % (link, format(rank,'.12f')))

	spark.stop()
