from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession



def computeContribs(values, rank):
    num_values = len(values)
    for value in values:
        yield (value[0], rank / num_values)

def computeAverages(avg):
    for l in avg:
        yield (l[0],l[1])

def parseNeighbors(urls):
    parts = re.split(',', urls)
    return (parts[0], (parts[1],(float(parts[2])/float(parts[3]))))

if __name__ == "__main__":
	spark = SparkSession    .builder    .appName("PythonPageRank")    .getOrCreate()


	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
	ranks = links.flatMap(lambda url_urls_rank: url_urls_rank[1]).reduceByKey(add).mapValues(lambda x: max(x,1))

	defaultRanks = 0.8
	if(float(sys.argv[3]) != 0.0):
		defaultRanks = float(sys.argv[3])

	if(int(sys.argv[2])==0):
		converged = False
		count = 1
		while(not converged):
			contribs = links.join(ranks).flatMap(
				lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			next_rank = contribs.reduceByKey(add).mapValues(lambda rank: rank * defaultRanks + 1.0 - defaultRanks)
			anyCon = True
			l = next_rank.join(ranks).map(lambda y: abs(y[1][0]-y[1][1])).filter(lambda x: x >= 0.0001)
			if(l.count() >0):
				anyCon = False
			converged = anyCon 
			ranks = next_rank
			count+=1
	else:
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(
			lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * defaultRanks + 1.0 -defaultRanks)


	sortedRanks = ranks.mapValues(lambda y: round(y,12)).sortBy(lambda x: (-x[1],x[0])).collect()
	for i in sortedRanks:
		print("%s, %s" % (i[0], str(i[1]).ljust(14, '0') ))
