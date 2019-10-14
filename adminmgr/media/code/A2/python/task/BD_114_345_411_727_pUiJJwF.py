#! bin/bash/python3
from __future__ import print_function
import re
import sys
from operator import add
import os
#os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.5'
from pyspark.sql import SparkSession

flag = 0

def compute_contribs(urls, rank):
	num_urls = len(urls)
	for url in urls:
		yield (url, float(rank / num_urls))

def assign(urls):
	parts = re.split(r',', urls)
	return (parts[0], float(int(parts[2])/int(parts[3])))

def parse_neighbours(urls):
	parts = re.split(r',', urls)
	return (parts[0], parts[1])

def average(pres,  prev):
	return abs(prev - pres)

if (__name__ == '__main__'):
	if (len(sys.argv) != 4):
		print("Usage: pagerank <file>", file = sys.stderr)
		sys.exit(-1)
	spark = SparkSession\
		.builder\
		.appName("PythonPageRank")\
		.getOrCreate()
	
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: (r[0]))
	initial_rank = lines.map(lambda urls: assign(urls)).distinct().groupByKey().cache()
	links = lines.map(lambda urls: parse_neighbours(urls)).distinct().groupByKey().cache()
	res = initial_rank.mapValues(sum)
	ranks = res.map(lambda url_neighbours: (url_neighbours[0], max(url_neighbours[1], 1.0)))
	weight = float(int(sys.argv[3]) / 100)
	if (int(sys.argv[2]) != 0):
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(lambda url_rank: compute_contribs(url_rank[1][0], url_rank[1][1]))
			
			if (weight == 0):
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
			else:
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1 - weight))
	else:
		while(1):
			contribs = links.join(ranks).flatMap(lambda url_rank: compute_contribs(url_rank[1][0], url_rank[1][1]))
			prev = ranks
			if (weight == 0):
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
			else:
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1 - weight))
			pres = ranks
			rank = prev.join(pres).mapValues(lambda url: average(url[0], url[1]))
			maximum = rank.reduce(max)
			if (maximum[1] < 0.0001):
				break
				
	ranks = ranks.sortBy(lambda x: -x[1])
	for (link, rank) in ranks.collect():
		print("%s,%.12f" % (link, float(rank)))
	spark.stop()
