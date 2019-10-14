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
	parts = re.split(r'\s+', urls)
	return parts[0], parts[1]




if __name__ == "__main__":
	'''if len(sys.argv) != 3:
		print("Usage: pagerank <file> <iterations>", file=sys.stderr)
		sys.exit(-1)'''


	spark = SparkSession\
		.builder\
		.appName("PythonPageRank")\
		.getOrCreate()
	
	
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	batbol=lines.map(lambda x:((x.split(",")[0],x.split(",")[1])))
	batbolgrp=batbol.groupByKey()



	initialrank=lines.map(lambda x:(x.split(",")[0],float(x.split(",")[2])/float(x.split(",")[3])))
	#print(initialrank.collect())
	aggrank=initialrank.reduceByKey(lambda x,y:x+y)
	#print(aggrank.collect())
		
	ranks=aggrank.map(lambda x:(x[0],max(x[1],1.0)))
	limit=int(sys.argv[2])
	
	
	links = batbolgrp
	if(limit==0):
		old=ranks
		contribs = links.join(ranks).flatMap(
				lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)

		check=old.join(ranks).filter(lambda x : abs(x[1][1]-x[1][0])>0.0001)
		maxiter=0
		while not check.isEmpty() and maxiter<500:
			old=ranks
			maxiter+=1
			contribs = links.join(ranks).flatMap(
				lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
			check=old.join(ranks).filter(lambda x : abs(x[1][1]-x[1][0])>0.0001)
			
		
	
	else:
		w=sys.argv[3]
		w=float(w)*.01
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(
				lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * w + (1-w))
	
	
	ranks=sorted(ranks.collect(),key=lambda x:(-x[1],x[0]))
	for (link,rank) in ranks:
		print("%s,%.12f" % (link,rank))

	'''for (link, rank) in ranks.collect():
		print("%s has rank: %s." % (link, rank))'''

	

	spark.stop()




















	
	
