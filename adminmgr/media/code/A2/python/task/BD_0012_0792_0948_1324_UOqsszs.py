from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession

def computeContribs(url, rank):
	num_urls = len(url)
	for i in url:
        	yield (i, float(rank)/ float(num_urls))

def parseNeighbors(url):
	prts = re.split(r',', url)
	avg = float(prts[2])/float(prts[3])
	return prts[0],avg

def parseNeighbors1(url):
	prts = re.split(r',', url)
	return prts[0], prts[1]

def converge(o_rank,n_rank):
	#o=o_rank.collect()
	#n=n_rank.collect()
	cnt=0
	#has_converged = 0
	i = 0
	rank_sort1 = sorted(o_rank.collect(),key = lambda a: (-a[1],a[0]))
	rank_sort2 = sorted(n_rank.collect(),key = lambda a: (-a[1],a[0]))
	#while (i == 0): 
	if (abs(rank_sort2[0][1]-rank_sort1[0][1]) < 0.0001):
		cnt+=1
			#has_converged &= 1
		#i=1
		return 1
	else:
		#has_converged &= 0
		return 0
	#return has_converged

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
	ranks1 = linss.map(lambda url: parseNeighbors(url)).distinct().groupByKey().mapValues(sum)
	ranks = ranks1.map(lambda x:(x[0],max(x[1],1)))
	links = linss.map(lambda url: parseNeighbors1(url)).distinct().groupByKey().cache()

	weight=int(sys.argv[3])/100
	if(weight == 0):
		weight = 0.8
	
	num_iters = 0
	if(int(sys.argv[2]) == 0):
		contributs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
		ranks = contributs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1-weight))
		o_rank=ranks
		i=0
		while (i == 0 ):
			num_iters+=1
			contributs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contributs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1-weight)) 
			n_rank=ranks
			if converge(o_rank,n_rank):
				i=1
			o_rank = n_rank

	else:
		for iteration in range(int(sys.argv[2])):
			contributs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contributs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1-weight)) 

	rank_sort = sorted(ranks.collect(), key = lambda a: (-a[1],a[0]))

	for (link, rank) in rank_sort:
		print("%s,%s" % (link, format(rank,'.12f')))

	spark.stop()
