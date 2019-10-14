from __future__ import print_function
import re
import sys
from operator import add

from pyspark.sql import SparkSession


def bowlingaverage(bats):
	mysum=0     
	for b in bats:
		rns=float(b[1])
		d=float(b[2])
		avg=rns/d
		mysum+=avg
	return mysum

def computeContribs(urls, rank):
	num_urls = len(urls)
	for url in urls:
		yield (url[0], rank / num_urls)

def parseNeighbors(urls):
	parts = re.split(r'\,', urls)
	return parts[0],(parts[1],parts[2],parts[3])


if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: pagerank <file> <iterations> ", file=sys.stderr)
		sys.exit(-1)

	spark = SparkSession\
		.builder\
		.appName("PythonPageRank")\
		.getOrCreate()

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	#lines = str(lines).split(',')
	#print("######",lines)  
	temp=float(sys.argv[3])
	if temp==0.0:
		temp=0.8
	else:
		temp=temp/100.0
    
	links = lines.map(lambda urls: parseNeighbors(urls)).groupByKey().cache()
   
    #for link in links.collect():
    #  print("@@@@@@ ",link[0],list(link[1]))

	ranks = links.map(lambda url_neighbors: (url_neighbors[0], max(1.0,bowlingaverage(list(url_neighbors[1])))))
	itt=0
	if(int(sys.argv[2])==0):
		while 1:
			prev=ranks
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank : rank * temp + (1-temp))
			diff=prev.join(ranks).map(lambda x:abs(x[1][0]-x[1][1]))
			t=diff.filter(lambda x:x<0.0001).count()
			if t==ranks.count():
				break    
	else:
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank : rank * temp + (1-temp))
	    
	ranks = ranks.sortBy(lambda x : (-x[1],x[0]))

	for (bowler, rank) in ranks.collect():
		print("%s,%.12f"%(bowler,rank))


   

	spark.stop()
