#!/usr/bin/python
from __future__ import print_function

import re
import sys
from operator import add

import pyspark
import pyspark.sql.functions as sf
import pyspark.sql.types as sparktypes

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url[0], rank / num_urls)
def parseAverage(urls):
    parts = re.split(r',', urls)
    avg=float(parts[2])/float(parts[3])
    yield (parts[1],avg)

def parseNeighbors(urls):
    parts = re.split(r',', urls)
    avg=float(parts[2])/float(parts[3])
    return parts[0],(parts[1],avg)




if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: pagerank <file> <iterations>", file=sys.stderr)
		sys.exit(-1)
	
	itr = int(sys.argv[2])
	wht = int(sys.argv[3])
	
	# Initialize the spark context.
	spark = SparkSession\
	.builder\
	.appName("BowlerPageRank")\
	.getOrCreate()

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
	ranks = lines.flatMap(lambda names: parseAverage(names)).reduceByKey(add).map(lambda y: (y[0],max(y[1],1.0)))
	
	convergeds = True
	count = 1
	
	if(itr == 0):
		if(wht == 0):	
			while(convergeds):
				contribs = links.join(ranks).flatMap(
			   	 lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
				next_rank = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
				convergeds = False
				rnks = sorted(ranks.collect(), key = lambda x: x[0])
				nextrnks = sorted(next_rank.collect(), key = lambda x: x[0])
				for i in range(0,ranks.count()):
					if(abs(rnks[i][1]-nextrnks[i][1])> 0.0001):
						convergeds  = True
						break
				#print(count)
				count+=1	
				ranks = next_rank

		else:
			while(convergeds):
				contribs = links.join(ranks).flatMap(
			   	 lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
				next_rank = contribs.reduceByKey(add).mapValues(lambda rank: rank *(wht/100) + ((100-wht)/100))
				convergeds = False
				rnks = sorted(ranks.collect(), key = lambda x: x[0])
				nextrnks = sorted(next_rank.collect(), key = lambda x: x[0])
				for i in range(0,ranks.count()):
					if(abs(rnks[i][1]-nextrnks[i][1])> 0.0001):
						convergeds  = True
						break
				#print(count)
				count+=1	
				ranks = next_rank

		
	else:
		if(wht == 0):
			while(convergeds and (count!=itr)):
				contribs = links.join(ranks).flatMap(
			   	 lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
				next_rank = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
				convergeds = False
				rnks = sorted(ranks.collect(), key = lambda x: x[0])
				nextrnks = sorted(next_rank.collect(), key = lambda x: x[0])
				for i in range(0,ranks.count()):
					if(abs(rnks[i][1]-nextrnks[i][1])> 0.0001):
						convergeds  = True
						break
				#print(count)
				count+=1	
				ranks = next_rank
		else:
			while(convergeds and (count!=itr)):
				contribs = links.join(ranks).flatMap(
			   	 lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
				next_rank = contribs.reduceByKey(add).mapValues(lambda rank: rank *(wht/100) + ((100-wht)/100))
				convergeds = False
				rnks = sorted(ranks.collect(), key = lambda x: x[0])
				nextrnks = sorted(next_rank.collect(), key = lambda x: x[0])
				for i in range(0,ranks.count()):
					if(abs(rnks[i][1]-nextrnks[i][1])> 0.0001):
						convergeds  = True
						break
				#print(count)
				count+=1	
				ranks = next_rank				
		
			
	#print(ranks.collect())
	for (key, value) in sorted(ranks.collect(), key = lambda x: x[1], reverse = True):
		print("%s , %13.12f " % (key, value))

	spark.stop()
