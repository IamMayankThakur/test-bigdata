from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession

def computeContribs(urls, rank):
	num_urls = len(urls)
	for url in urls:
        	yield (url, rank / num_urls)

def parseNeighbors(urls):
	splits = re.split(r',', urls)
	a=int(splits[2])
	b=int(splits[3])

	return splits[1],(a)/(b)

def parseNeighbors1(urls):
	splits = re.split(r',', urls)
	return splits[0], splits[1]

def converge(old_ranks,new_ranks):
	old=old_ranks.collect()
	new=new_ranks.collect()
	count=0
	has_converged = 1
	i = 0
	while i < len(new) and not has_converged:
		if (new[i][1]-old[i][1] < 0.0001):
			count+=1
			has_converged &= 1
		else:
			has_converged &= 0
		i += 1
	
	return has_converged

if __name__ == "__main__":
	
	if len(sys.argv) != 4:
        	print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        	sys.exit(-1)



	spark = SparkSession\
		.builder\
		.appName("PythonPageRank")\
		.getOrCreate()
	#print("Read")
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	ranks = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().mapValues(sum).cache()
	ranks_new=ranks.mapValues(lambda c:max(c,1.0))
	links = lines.map(lambda urls: parseNeighbors1(urls)).distinct().groupByKey().cache()

	fourvalue=float(sys.argv[3])
	num_iter = 0
	#print("Begin")
	if(int(sys.argv[2]) == 0):
		old_rank=None
		while True:
			num_iter+=1
			contribs = links.join(ranks_new).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			old_rank=ranks_new
			
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * fourvalue + 1-fourvalue)
			new_rank=ranks
			
			if converge(old_rank,new_rank) :
				break
			

	elif(int(sys.argv[2]) > 0):
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			
	for (link, rank) in ranks.collect():
		
		print("%s,%s" % (link,rank))
	
	
	
	
	
	



	spark.stop()
