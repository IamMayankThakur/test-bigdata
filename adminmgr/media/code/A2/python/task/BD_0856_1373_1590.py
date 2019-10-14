from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseRanks(urls):
	parts = re.split(r',', urls)
	a=(float(parts[2])/float(parts[3]))

	return parts[1], a

def parseNeighbors(urls):
    parts = re.split(r',', urls)
    return parts[0], parts[1]





if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: pagerank <file> <num_iterations> <rank>", file=sys.stderr)
		sys.exit(-1)

    # Initialize the spark context.
	spark = SparkSession\
		.builder\
		.appName("PythonPageRank")\
		.getOrCreate()
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

	links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey()
	new_links = lines.map(lambda urls: parseRanks(urls)).distinct().groupByKey()
	ranks = new_links.map(lambda average: (average[0], max(1,sum(average[1]))))
	prev_ranks = new_links.map(lambda average: (average[0], max(1,sum(average[1]))))
	weightage = int(sys.argv[3])

	if weightage == 0:
		weightage=0.8
	else:
		weightage=float((int(sys.argv[3]))/100)
		c=(1-weightage)
	c=(1-weightage)
	
	num_iteration=int(sys.argv[2])
	i=0
	if(num_iteration>0):
		for i in range(num_iteration):
			contribs = links.join(ranks).flatMap(
				lambda url_ranks: computeContribs(url_ranks[1][0], url_ranks[1][1]))

			ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank *weightage + c))   
	else:
		b=1
		while(b):
            
			i=i+1
			contribs = links.join(ranks).flatMap(
				lambda url_ranks: computeContribs(url_ranks[1][0], url_ranks[1][1]))
			prev_ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank *weightage + c))

			sec_rank =ranks.join(prev_ranks)
			diff = [abs(n[0]-n[1]) for (b,n) in sec_rank.collect()]

			if((max(diff)<0.0001)):
				b=0
			else:
				ranks = prev_ranks
		
  
	ranks = ranks.sortBy(lambda a : a[0])  
	prev_ranks = ranks.sortBy(lambda a : -a[1])

	for (link, rank) in prev_ranks.collect():
		print("%s,%s" % (link, rank))
	
    
	spark.stop()



