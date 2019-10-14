from __future__ import print_function
import re
import sys
from operator import add
from pyspark.sql import SparkSession

def computeContribs(urls, rank):
	num_urls = len(urls)
	for url in urls:
		yield (url, float(rank / num_urls))

def parseNeighbors(urls):
	parts = re.split(',', urls)
	return parts[0], parts[1]

def abc(urls):
	parts = re.split(',', urls)
	return parts[1],float(float(parts[2])/float(parts[3]))

if __name__ == "__main__":
	spark = SparkSession\
	    .builder\
	    .appName("PythonPageRank")\
	    .getOrCreate()
	
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	links = lines.map(lambda urls: parseNeighbors(urls)).groupByKey().cache()
	tp =lines.map(lambda urls: abc(urls)).reduceByKey(add).cache()
	ranks = tp.map(lambda url_neighbors: (url_neighbors[0], max(1.0,url_neighbors[1])))
	ranks1=ranks
	
	if(int(sys.argv[2])!=0):
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			if(int(sys.argv[3])!=0):
				x=int(sys.argv[3])/100
				y=1-(int(sys.argv[3])/100)
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank * x) + y)
			else:
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank * 0.80) + 0.20)
		
		ranks=ranks.sortBy(lambda x: x[0],True)
		ranks=ranks.sortBy(lambda x: x[1],False)
		
		for (link, rank) in ranks.collect():
			print("%s,%.12f" % (link, float(rank)))
		spark.stop()
	
	else:
		while(1):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			
			if(int(sys.argv[3])!=0):
				x=int(sys.argv[3])/100
				y=1-(int(sys.argv[3])/100)
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank * x) + y)
			else:
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank * 0.80) + 0.20)
			
			ranks_joined = ranks.join(ranks1)
			flag=0
			
			for(link,(rank1,rank2)) in ranks_joined.collect():
				if(abs(float(rank1)-float(rank2))>=0.0001):
					flag=1
					break
				
			if(flag==1):
				ranks1=ranks

			if(flag==0):
				break

		ranks_joined=ranks_joined.sortBy(lambda x: x[0],True)
		ranks_joined=ranks_joined.sortBy(lambda x: x[1],False)
		
		for (link, (rank1,rank2)) in ranks_joined.collect():
			print("%s,%.12f" % (link, float(rank1)))
		spark.stop()