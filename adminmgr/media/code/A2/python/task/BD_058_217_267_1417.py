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
    parts = urls.split(',')
    return parts[0], parts[1]
    
def func(urls):
    parts = urls.split(',')
    return parts[1], (int(parts[2]), int(parts[3]))

def diff(xy,yz):
	a = xy.join(yz).map(lambda abc: abs(abc[1][0]-abc[1][1]))
	#a.saveAsTextFile("hdfs://localhost:9000/spark/testout13.txt")
	m = a.reduce(lambda x,y: max(x,y))
	return m

if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: pagerank <file> <iterations>", file=sys.stderr)
		sys.exit(-1)

    # Initialize the spark context.
	spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

	links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
	#links.saveAsTextFile("hdfs://localhost:9000/spark/testout8.txt")
	initrank = lines.map(lambda urls: func(urls)).distinct()
	ranks1 = initrank.map(lambda urls: (urls[0],urls[1][0]/urls[1][1])).reduceByKey(add)
	ranks = ranks1.map(lambda urls: (urls[0],max(1,urls[1])))
	#ranksinit = ranks
	#initrank = lines.map(lambda urls: func(urls)).distinct().reduceByKey(lambda x,y:(int(x[0])+int(y[0]),int(x[1])+int(y[1])))
	#initrank.saveAsTextFile("hdfs://localhost:9000/spark/testout3.txt")
	#ranks = initrank.map(lambda urls: (urls[0],max(1,urls[1][0]/urls[1][1])))
	#ranks.saveAsTextFile("hdfs://localhost:9000/spark/testout6.txt")
	check = ranks  
	if(int(sys.argv[3])==0):
		weight = 0.8
	else:
		weight = (int(sys.argv[3]))/100
        #ranks = links.map(lambda url_neighbors: (url_neighbors[0], max(1.0, lines.)))
	#con = check.join(ranks)
	#con.saveAsTextFile("hdfs://localhost:9000/spark/testout10.txt")
	#for iteration in range(int(sys.argv[2])):
	if(int(sys.argv[2]) == 0):
		running = True
		while(running):
			contribs = links.join(ranks).flatMap(
				lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*weight + (1-weight))
			#ranks.saveAsTextFile("hdfs://localhost:9000/spark/testout20.txt")
			if(diff(check,ranks)<0.0001):
				running = False
				break
			check = ranks
		#running = False
	else:
		for i in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(
				lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*weight + (1-weight))

	ranks = ranks.sortBy(lambda y: y[0])
	ranks = ranks.sortBy(lambda x: x[1],ascending = False)

	for (link, rank) in ranks.collect():
		print("%s,%s" % (link, round(rank,12)))

	spark.stop()

