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
	parts = re.split(r',', urls)
	b_average = int(parts[2])/int(parts[3])
	return parts[1],b_average

def parseNeighbors1(urls):
	parts = re.split(r',', urls)
	return parts[0], parts[1]
'''
def convergence(conv_array,ranks):
	t=1
	j=0
	while(t):
		for(i,k) in ranks.collect():
			if(float(k)-conv_array[j]<0.0001):
				t=0
				break

			j+=1
	if not t:
		check = [float(k) for i,k in ranks.collect]
		return 1
	return 0
'''
def converge(old_rank,new_rank):
	old=old_rank.collect()
	new=new_rank.collect()
	count=0
	has_converged = 1
	i = 0
	while i < len(new) and not has_converged:
		if (new[i][1]-old[i][1] < 0.0001):
			count+=1
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
	print("Read")
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	ranks = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().mapValues(sum).cache()
	ranks_new=ranks.mapValues(lambda x:max(x,1.0))
	links = lines.map(lambda urls: parseNeighbors1(urls)).distinct().groupByKey().cache()

	weight=float(sys.argv[3])
	num_iter = 0
	print("Begin")
	if(int(sys.argv[2]) == 0):
		old_rank=None
		while True:
			num_iter+=1
			contribs = links.join(ranks_new).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			old_rank=ranks_new
			print("check 1")
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + 1-weight)
			new_rank=ranks
			print("check 2")
			print(num_iter)
			if converge(old_rank,new_rank) :
				break
		print("num_iter :",num_iter)

	elif(int(sys.argv[2]) > 0):
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + 1-weight)

	rank_sort = ranks.sortBy(lambda a: -a[1])

	for (link, rank) in rank_sort.collect():
		print("%s,%s." % (link, format(rank,'.12f')))

	spark.stop()
