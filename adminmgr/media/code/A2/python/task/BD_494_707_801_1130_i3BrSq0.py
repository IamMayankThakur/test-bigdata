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
	return parts[0], parts[1]

def abc(urls):
	parts = re.split(',', urls)
	return parts[1],float(parts[2])/float(parts[3])

if __name__ == "__main__":
	spark = SparkSession\
	    .builder\
	    .appName("PythonPageRank")\
	    .getOrCreate()
	
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
	tp =lines.map(lambda urls: abc(urls)).reduceByKey(add).cache()
	ranks = tp.map(lambda url_neighbors: (url_neighbors[0], max(1.0,url_neighbors[1])))
	ranks1=ranks
	count=0
	
	if(int(sys.argv[2])!=0):
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			if(int(sys.argv[3])!=0):
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * int(sys.argv[3])/100 + 1-(int(sys.argv[3])/100))
			else:
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80+0.20)
		for (link, rank) in ranks.collect():
			print("%s has rank: %s." % (link, rank))
		spark.stop()
		exit()
	
	else:
		while(1):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			
			if(int(sys.argv[3])!=0):
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * int(sys.argv[3])/100 + 1-(int(sys.argv[3])/100))
			else:
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80+0.20)
				
			flag=1
			for i,j in zip(ranks.collect(),ranks1.collect()):
				if(abs(float(i[1])-float(j[1]))>=0.0001):
                	flag=0
			
			if(flag==1):
                break
            
			else:
                ranks1=ranks
				
		ranks=ranks.sortByKey().sortBy(lambda x: x[0],True)
		ranks=ranks.sortByKey().sortBy(lambda x: x[1],False)
		for (link, rank) in ranks.collect():
			print("%s has rank: %s." % (link, rank))
		spark.stop()
		exit()