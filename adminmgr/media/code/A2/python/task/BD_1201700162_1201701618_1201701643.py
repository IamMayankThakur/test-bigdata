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
    parts = re.split(',', urls)
    return parts[0],parts[1]

def findaverage(urls):
    parts = re.split(',', urls)
    return parts[1], float(parts[2])/float(parts[3])


if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: pagerank <file> <iterations> <weight>", file=sys.stderr)
		sys.exit(-1)
	spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey()
	linksn = lines.map(lambda urls: findaverage(urls)).distinct().groupByKey()
	ranks = linksn.map(lambda url_neighbors: (url_neighbors[0],max(1.0,sum(url_neighbors[1]))))
	convergence=True
	count=0
	itn=int(sys.argv[2])
	weight=int(sys.argv[3])/100
	we=1-weight
	if itn==0:
		while(convergence):
			oldr=links.join(ranks).map(lambda rankno:(rankno[0],rankno[1][1])).sortByKey()
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank:rank*weight+we)
			newr=ranks.map(lambda newrank:(newrank[0],newrank[1])).sortByKey()
			check=oldr.join(newr).map(lambda x:abs(x[1][1]-x[1][0])).filter(lambda x:x>0.0001).collect()
			if(len(check)>0):
				convergence=True
			else:
				convergence=False
			count=count+1
	else:
		for iteration in range(itn):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*weight+we)
	for(link, rank) in ranks.sortBy(lambda a:(a[1],a[0]),False).collect():
		print("%s,%s." % (link, rank))
	#print(count) 			
	spark.stop()
