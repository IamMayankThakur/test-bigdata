from __future__ import print_function
import itertools
import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
	num_urls = len(urls)
	for url in urls:
        	yield (url, rank / num_urls)


def parseNeighbors(urls):
	parts = re.split(r',', urls)
	return parts[0],parts[1]

def func(urls):
	urls1 = re.split(r',', urls)
	return ((urls1[1],float(urls1[2])/float(urls1[3])))        

#def converge(temp,ranks):
#	return ranks.join(temp).flatMap(lambda r:r[1][0]-r[1][1])
    
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
    #links = lines.map(lambda urls: parseNeighbors(urls))
	forranks = lines.map(lambda urls: func(urls)).reduceByKey(add)

	ranks = forranks.map(lambda url_neighbors: (url_neighbors[0],max(float(1),url_neighbors[1])))
	if int(sys.argv[2])!=0:
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			if int(sys.argv[3])==0:
				rank = contribs.reduceByKey(add).mapValues(lambda r: r * 0.8 + 0.2)
			else:
				rank = contribs.reduceByKey(add).mapValues(lambda r: r * int(sys.argv[3])/100 + (100-int(sys.argv[3])/100))
		new_ranks=rank.sortBy(lambda j:(-j[1],j[0]))
		for (link, rank) in new_ranks.collect():
			print(link,"{:.12f}".format(rank),sep=",")
	else:
		flag1=1
		
		for elt in itertools.count():
			
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			if int(sys.argv[3])==0:
				rank = contribs.reduceByKey(add).mapValues(lambda r: r * 0.8 + 0.2)
			else:
				rank = contribs.reduceByKey(add).mapValues(lambda r: r * int(sys.argv[3])/100 + (100-int(sys.argv[3])/100))
			flag1=0
			a=sorted(ranks.collect(),key=lambda x:x[0])
			b=sorted(rank.collect(),key=lambda x:x[0])
			#print(elt)
			
			#while(i<ranks.count()):
			#	if(abs((a[i][1])-(b[i][1]))>0.0001):
			#		flag1=1
			#		break
			#	i=i+1
			#ranks=rank
			for i in range(0,ranks.count()):
				if(abs((a[i][1])-(b[i][1]))>0.0001):
					flag1=1
					break
			ranks=rank	
			
			if flag1==0:
				break
		new_ranks=ranks.sortBy(lambda j:(-j[1],j[0]))
		for (link, rank) in new_ranks.collect():
			print(link,"{:.12f}".format(rank),sep=",")

	spark.stop()

