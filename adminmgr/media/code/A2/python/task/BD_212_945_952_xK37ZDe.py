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
    parts = re.split(r'\,', urls)
    return parts[0], parts[1]

def bowavg(a):
	parts = re.split(r'\,', a)
	t=int(parts[2])/int(parts[3])
	return parts[0],t
	
def converge(a,b):
	if(abs((a-b)) < 0.0001):
		return 1
	else:
		return 0

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

	links = lines.map(lambda urls1: parseNeighbors(urls1)).distinct().groupByKey()

	links1 = lines.map(lambda urls2: bowavg(urls2)).reduceByKey(add)

	ranks = links1.map(lambda urls3: (urls3[0], max(urls3[1],1.0)))

	#c = 0
	iterate = int(sys.argv[2])
	if(iterate > 0):
		for iteration in range(iterate):
			contribs = links.join(ranks).flatMap(
				lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			
			if(int(sys.argv[3])>0):
				w = int(sys.argv[3])/100
				ranks1 = contribs.reduceByKey(add).mapValues(lambda rank: rank * w + (1-w))
			else:
				ranks1 = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
			ranks = ranks1
	else:
		flag =1
		
		while(flag):
			contribs1 = links.join(ranks).flatMap(
				lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			
			if(int(sys.argv[3])>0):
				w = int(sys.argv[3])/100
				ranks1 = contribs1.reduceByKey(add).mapValues(lambda rank: rank * w + (1-w))
				contribs2 = links.join(ranks1).flatMap(
				lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
				ranks2 = contribs2.reduceByKey(add).mapValues(lambda rank: rank * w + (1-w))
			else:
				ranks1 = contribs1.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
				contribs2 = links.join(ranks1).flatMap(
				lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
				ranks2 = contribs2.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
			
			t= ranks1.join(ranks2)
			t = t.map(lambda urls: (urls[1][0]-urls[1][1])).map(lambda urls5: 1 if(urls5 < 0.0001) else 0)
			
			for i in t.collect():
				if(i==0):
					flag = 1
					ranks = ranks1
					ranks1 = ranks2
				else:
					flag = 0
			c=c+1
			
	#print("%d Count" % (c))		
	outsort1 = ranks1.sortBy(lambda a: a[0])
	outsort = outsort1.sortBy(lambda a: a[1],ascending = False)
	for (i,j) in (outsort.collect()):
		print("%s,%.12f" % (i, j))
	spark.stop()
